package model

import "sync"

// EquipmentState holds the dynamic state of a piece of equipment.
type EquipmentState struct {
	Name           string
	OperatingState string  // OFF, RUNNING, FAULT, DEGRADING
	Health         float64 // 1.0 is perfect, < 1.0 is degraded
	Load           float64 // 0.0 to 1.0
	// SensorFailures maps a PI Point ID to a failure mode (e.g., "stuck", "zero", "noisy").
	SensorFailures map[int]string
}

// PlantState holds the global state of the plant.
type PlantState struct {
	sync.RWMutex
	Equipment           map[string]*EquipmentState
	PlantLoad           float64 // Overall plant load demand (0.0 to 1.0)
	GridCarbonIntensity float64 // gCO2/kWh
}
