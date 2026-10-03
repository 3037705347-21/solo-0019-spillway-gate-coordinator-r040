package storage

import (
	"context"
	"sync"
	"time"

	"example.com/spillway-gate-coordinator/internal/domain"
)

type MemoryRepository struct {
	mu       sync.RWMutex
	gates    map[string]domain.Gate
	commands map[string]domain.GateCommand
	stations map[string]*stationDriveLedger
}

// stationDriveLedger 记录一座泵站当前被在途调整占用的行程额度。
type stationDriveLedger struct {
	inUse    int
	bookings map[string]int
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		gates:    make(map[string]domain.Gate),
		commands: make(map[string]domain.GateCommand),
		stations: make(map[string]*stationDriveLedger),
	}
}

func (m *MemoryRepository) CreateGate(_ context.Context, gate domain.Gate) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.gates[gate.ID]; exists {
		return domain.Conflict("duplicate_gate_id", "gate id already exists")
	}
	if _, exists := m.stations[gate.Station]; !exists {
		m.stations[gate.Station] = &stationDriveLedger{bookings: make(map[string]int)}
	}
	m.gates[gate.ID] = gate
	return nil
}

func (m *MemoryRepository) Gate(_ context.Context, id string) (domain.Gate, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	gate, exists := m.gates[id]
	if !exists {
		return domain.Gate{}, domain.NotFound("gate_not_found", "gate id was not found")
	}
	return gate, nil
}

func (m *MemoryRepository) CreateCommand(_ context.Context, command domain.GateCommand) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	gate, exists := m.gates[command.GateID]
	if !exists {
		return domain.NotFound("gate_not_found", "gate id was not found")
	}
	if gate.Status == domain.GateFaulted {
		return domain.Conflict("gate_faulted", "faulted gates cannot receive new commands")
	}
	if command.StartingAperture != gate.Aperture {
		return domain.Conflict("gate_aperture_changed", "gate aperture changed before the command was stored")
	}
	if command.TargetAperture > gate.MaxAperture {
		return domain.Invalid("invalid_target_aperture", "target aperture is outside the gate limit")
	}
	if _, exists := m.commands[command.ID]; exists {
		return domain.Conflict("duplicate_command_id", "command id already exists")
	}

	m.commands[command.ID] = command
	return nil
}

func (m *MemoryRepository) Command(_ context.Context, id string) (domain.GateCommand, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	command, exists := m.commands[id]
	if !exists {
		return domain.GateCommand{}, domain.NotFound("command_not_found", "command id was not found")
	}
	return cloneCommand(command), nil
}

// canAcquireDriveLocked 仅检查泵站剩余额度是否足够，不改动账本。
func (m *MemoryRepository) canAcquireDriveLocked(station string, units int) bool {
	ledger, exists := m.stations[station]
	if !exists {
		return units <= domain.StationDriveBudget
	}
	return ledger.inUse+units <= domain.StationDriveBudget
}

// commitDriveBookingLocked 在提交点把一次调整预留的行程量记入泵站共享账本。
func (m *MemoryRepository) commitDriveBookingLocked(station, commandID string, units int) {
	ledger, exists := m.stations[station]
	if !exists {
		ledger = &stationDriveLedger{bookings: make(map[string]int)}
		m.stations[station] = ledger
	}
	if units < 1 {
		units = 1
	}
	ledger.bookings[commandID] = units
	ledger.inUse += units
}

// releaseDriveLocked 按登记时预留的行程量归还额度；返回当初预留的精确数量。
// 若该命令没有任何在途占用（例如复核从未成立），不改动账本。
func (m *MemoryRepository) releaseDriveLocked(station, commandID string) int {
	ledger, exists := m.stations[station]
	if !exists {
		return 0
	}
	units, booked := ledger.bookings[commandID]
	if !booked {
		return 0
	}
	delete(ledger.bookings, commandID)
	ledger.inUse -= units
	if ledger.inUse < 0 {
		ledger.inUse = 0
	}
	return units
}

func (m *MemoryRepository) ReviewCommand(
	_ context.Context,
	commandID string,
	decision domain.InterlockDecision,
	token string,
	now time.Time,
) (domain.GateCommand, domain.Gate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	command, exists := m.commands[commandID]
	if !exists {
		return domain.GateCommand{}, domain.Gate{}, domain.NotFound("command_not_found", "command id was not found")
	}
	gate, exists := m.gates[command.GateID]
	if !exists {
		return domain.GateCommand{}, domain.Gate{}, domain.NotFound("gate_not_found", "gate id was not found")
	}

	// 先在副本上完成所有状态转换和额度核算，任何一步失败都不得触碰共享账本或已存记录。
	nextCommand := cloneCommand(command)
	if err := nextCommand.Review(decision, token, now); err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}

	driveUnits := 0
	nextGate := gate
	if decision.IsClear() {
		driveUnits = domain.TravelUnits(nextCommand.StartingAperture, nextCommand.TargetAperture)
		nextCommand.ReserveDriveUnits(driveUnits)
		if !m.canAcquireDriveLocked(gate.Station, driveUnits) {
			return domain.GateCommand{}, domain.Gate{}, domain.Conflict("station_drive_budget_exhausted", "station drive budget is exhausted")
		}
		if err := nextGate.Reserve(nextCommand.ID, now); err != nil {
			return domain.GateCommand{}, domain.Gate{}, err
		}
	}

	// 全部校验通过后才提交：额度占用、命令状态与闸门占用在同一临界区内一起写入。
	if decision.IsClear() {
		m.commitDriveBookingLocked(gate.Station, nextCommand.ID, driveUnits)
	}
	m.commands[nextCommand.ID] = nextCommand
	m.gates[nextGate.ID] = nextGate
	return cloneCommand(nextCommand), nextGate, nil
}

func (m *MemoryRepository) ExecuteCommand(
	_ context.Context,
	commandID string,
	token string,
	operator string,
	simulateFault bool,
	now time.Time,
) (domain.GateCommand, domain.Gate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	command, exists := m.commands[commandID]
	if !exists {
		return domain.GateCommand{}, domain.Gate{}, domain.NotFound("command_not_found", "command id was not found")
	}
	gate, exists := m.gates[command.GateID]
	if !exists {
		return domain.GateCommand{}, domain.Gate{}, domain.NotFound("gate_not_found", "gate id was not found")
	}

	nextCommand := cloneCommand(command)
	if err := nextCommand.BeginExecution(token, operator, now); err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}

	nextGate := gate
	if err := nextGate.StartMovement(nextCommand.ID, now); err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}

	if simulateFault {
		if err := nextGate.Settle(nextCommand.TargetAperture, true, now); err != nil {
			return domain.GateCommand{}, domain.Gate{}, err
		}
		if err := nextCommand.Abort("simulated_gate_fault", now); err != nil {
			return domain.GateCommand{}, domain.Gate{}, err
		}
	} else {
		if err := nextGate.Settle(nextCommand.TargetAperture, false, now); err != nil {
			return domain.GateCommand{}, domain.Gate{}, err
		}
		if err := nextCommand.Complete(now); err != nil {
			return domain.GateCommand{}, domain.Gate{}, err
		}
	}

	// 调整到达终态（完成或中止）后，按复核时预留的行程量从共享账本精确归还；
	// 归还数量以账本登记值为准，不受命令实体上字段清零的影响。
	m.releaseDriveLocked(nextGate.Station, nextCommand.ID)

	m.commands[nextCommand.ID] = nextCommand
	m.gates[nextGate.ID] = nextGate
	return cloneCommand(nextCommand), nextGate, nil
}

func cloneCommand(command domain.GateCommand) domain.GateCommand {
	if command.Interlock == nil {
		return command
	}
	decision := *command.Interlock
	command.Interlock = &decision
	return command
}
