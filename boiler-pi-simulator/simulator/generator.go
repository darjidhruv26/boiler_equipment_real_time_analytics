package simulator

import (
	"math/rand"
	"strings"
	"time"

	"boiler-pi-simulator/config"
	"boiler-pi-simulator/model"
)

var partitionMapping = map[string]int{
	// Turbine Equipment
	"HP Turbine":         0,
	"IP Turbine":         1,
	"LP Turbine":         2,
	"Turbine Rotor":      3,
	"Turbine Blades":     4,
	"Turbine Casings":    5,
	"Steam Valves":       6,
	"Turbine Bearings":   7,
	"Lubrication System": 8,
	"Turning Gear":       9,
	"Shaft Seals":        10,
	"Gland Seals":        10,
	"Turbine Protection": 11,
	"Control System":     12,

	// Boiler Equipment
	"Boiler Drum (Steam Drum)":       0,
	"Feedwater System":               1,
	"Furnace / Combustion Chamber":   2,
	"Superheater":                    3,
	"Economizer":                     4,
	"Air System (FD Fan / PA Fan)":   5,
	"Induced Draft Fan (ID Fan)":     6,
	"Fuel System (Coal / Gas / Oil)": 7,
	"Steam Output System":            8,
	"Safety and Protection":          9,

	// Generator Equipment
	"Generator":                   0,
	"Stator":                      1,
	"Rotor":                       2,
	"Exciter":                     3,
	"AVR":                         4,
	"Rotor Bearings":              5,
	"Hydrogen Cooling System":     6,
	"Hydrogen Gas Control Panel":  6,
	"Seal Oil System":             6,
	"Stator Cooling Water System": 7,
	"Generator Transformer":       8,
	"Neutral Grounding Equipment": 9,

	// Condenser Equipment
	"Condenser":             0,
	"Condenser Shell":       1,
	"Condenser Tubes":       2,
	"Water Box - Inlet":     3,
	"Water Box - Outlet":    4,
	"Tube Sheets":           5,
	"Hotwell":               6,
	"Air Extraction":        7,
	"Vacuum Breaking Valve": 8,
	"Condenser Auxiliary":   9,
	"Condenser Performance": 9,

	// Cooling Tower Equipment
	"Cooling Tower":               0,
	"Cooling Tower Structure":     1,
	"Fill Material":               2,
	"Drift Eliminators":           3,
	"Water Distribution System":   4,
	"Cooling Tower Basin":         5,
	"CT Fans":                     6,
	"CT Fan 1":                    7,
	"CT Fan 2":                    8,
	"CT Fan Motor":                9,
	"CT Gearbox":                  10,
	"CT Fan Blades":               11,
	"Make-up Water Valve":         12,
	"Bleed-off System":            13,
	"Louvers":                     14,
	"Cooling Tower Water Quality": 15,
	"Cooling Tower Performance":   16,

	// Coal Handling Equipment
	"Coal Handling":      0,
	"Wagon Tippler":      1,
	"Stacker-Reclaimer":  2,
	"Crusher":            3,
	"Vibrating Screen":   4,
	"Conveyor Belt":      5,
	"Magnetic Separator": 6,

	// Ash Handling Equipment
	"Ash Handling":            0,
	"Bottom Ash Hopper":       1,
	"Clinker Grinder":         2,
	"Ash Slurry Pump":         3,
	"Hydro-cyclone":           4,
	"Ash Silo":                5,
	"Dust Suppression System": 6,

	// Water Treatment Equipment
	"Water Treatment":     0,
	"Clarifier":           1,
	"Filter":              2,
	"DM Plant":            3,
	"Cation Exchanger":    4,
	"Anion Exchanger":     5,
	"Mixed Bed Exchanger": 6,
	"RO System":           7,
	"Degasser Tower":      8,
	"Chemical Dosing":     9,

	// Electrical System Equipment
	"Electrical System":          0,
	"Generator Transformer_el":   1,
	"Station Transformer":        2,
	"Switchyard":                 3,
	"Switchgear":                 4,
	"Motor Control Center":       5,
	"DC System":                  6,
	"Emergency Diesel Generator": 7,
	"Protection & Control":       8,

	// Instrumentation & Control Equipment
	"Instrumentation & Control": 0,
	"DCS":                       1,
	"PLC":                       2,
	"Field Sensors":             3,
	"Pressure Transmitter":      4,
	"Temperature Transmitter":   5,
	"Flow Meter":                6,
	"Level Transmitter":         7,
	"Actuator":                  8,
	"Control Valve":             9,
	"Vibration Monitoring":      10,
	"Vibration Sensor":          10,
	"Analyzer":                  11,
	"Control Room Console":      12,
}

func getPartition(equipment string) int {
	if p, ok := partitionMapping[equipment]; ok {
		return p
	}
	return 13 // Default
}

// generateStatefulValue creates realistic sensor data based on the global plant state.
func generateStatefulValue(pState *model.PlantState, tag config.TagMetadata) interface{} {
	pState.RLock()
	defer pState.RUnlock()

	// Fallback to a simple random value if needed
	defaultValue := generateValue(tag.Unit)

	eqState, ok := pState.Equipment[tag.Group]
	if !ok {
		return defaultValue
	}

	// 1. Handle specific sensor failures
	if failureMode, isFailed := eqState.SensorFailures[tag.PIPointID]; isFailed {
		switch failureMode {
		case "stuck":
			// Return a plausible but constant value for the unit
			return generateValue(tag.Unit)
		case "zero":
			return 0.0
		case "noisy":
			val, ok := generateValue(tag.Unit).(float64)
			if ok {
				// Add up to 50% random noise
				return val + (rand.Float64()-0.5)*val*0.5
			}
			return defaultValue // Fallback for non-float types
		}
	}

	// 2. Handle equipment operational state
	if eqState.OperatingState != "RUNNING" {
		if strings.Contains(tag.TagName, "_STATUS") {
			return 2 // Convention for STANDBY or OFF
		}
		if strings.Contains(tag.TagName, "_TEMP") {
			return 20.0 + rand.Float64()*10 // Ambient temperature
		}
		// Most values are zero if the equipment is off
		return 0.0
	}

	// 3. Apply load and health factors for normally running equipment
	val, isFloat := defaultValue.(float64)
	if !isFloat {
		return defaultValue // Return state values (0 or 1) as is
	}

	// Apply plant load factor (heuristic)
	loadFactor := 0.0
	tagNameUpper := strings.ToUpper(tag.TagName)
	if strings.Contains(tagNameUpper, "FLOW") || strings.Contains(tagNameUpper, "POWER") || strings.Contains(tagNameUpper, "CURRENT") || strings.Contains(tagNameUpper, "LOAD") {
		loadFactor = 1.0 // Directly proportional to load
	} else if strings.Contains(tagNameUpper, "PRESSURE") || strings.Contains(tagNameUpper, "SPEED") || strings.Contains(tagNameUpper, "RPM") {
		loadFactor = 0.7 // Mostly proportional
	} else if strings.Contains(tagNameUpper, "TEMP") && !strings.Contains(tagNameUpper, "BEARING") {
		loadFactor = 0.6 // Somewhat proportional
	}

	// Apply a simple load model: value = base * (min_operating_level + load * (1 - min_operating_level))
	if loadFactor > 0 {
		minOperatingLevel := 1.0 - loadFactor*0.8 // e.g., 20% base for full load factor
		val = val * (minOperatingLevel + pState.PlantLoad*(1.0-minOperatingLevel))
	}

	// Apply equipment health factor (adds noise and potential offset as health degrades)
	if eqState.Health < 1.0 {
		// Introduce more noise as health drops
		healthNoise := (1.0 - eqState.Health) * (rand.Float64() - 0.5) * (val * 0.05) // Up to 5% noise at 0 health
		val += healthNoise

		// Introduce a slight downward bias for efficiency-related tags
		if strings.Contains(tagNameUpper, "EFFICIENCY") {
			val *= (eqState.Health*0.2 + 0.8) // Degrade to 80% of value at 0 health
		}
	}

	return val
}

// GenerateGridCarbonEvents creates events for the grid carbon intensity topic.
func GenerateGridCarbonEvents(pState *model.PlantState) []model.SensorEvent {
	pState.RLock()
	carbonIntensity := pState.GridCarbonIntensity
	pState.RUnlock()

	event := model.SensorEvent{
		EventTime:   time.Now().UTC(),
		PIPointID:   99999, // A special, reserved ID for this metric
		TagName:     "GRID_CARBON_INTENSITY",
		EquipmentID: "GRID",
		Value:       carbonIntensity,
		Quality:     0,
		Topic:       "grid-carbon-intensity",
		Partition:   0,
	}
	return []model.SensorEvent{event}
}

func generateValue(unit string) interface{} {
	switch unit {
	case "State":
		if rand.Float32() > 0.95 {
			return 1 // Rare warning state
		}
		return 0
	case "°C":
		return 100.0 + rand.Float64()*300.0
	case "bar", "mbar":
		return 1.0 + rand.Float64()*150.0
	case "RPM":
		return 2950.0 + rand.Float64()*100.0
	case "%":
		return rand.Float64() * 100.0
	case "mm/s":
		return rand.Float64() * 5.0
	default:
		return rand.Float64() * 100.0
	}
}

func GenerateBoilerEvents(pState *model.PlantState) []model.SensorEvent {
	var events []model.SensorEvent
	now := time.Now().UTC()

	for _, t := range config.BoilerTags {
		events = append(events, model.SensorEvent{
			EventTime:   now,
			PIPointID:   t.PIPointID,
			TagName:     t.TagName,
			EquipmentID: t.Equipment,
			Value:       generateStatefulValue(pState, t),
			Quality:     0,
			Topic:       "boiler-sensors",
			Partition:   getPartition(t.Equipment),
		})
	}
	return events
}

func GenerateTurbineEvents(pState *model.PlantState) []model.SensorEvent {
	var events []model.SensorEvent
	now := time.Now().UTC() // UTC matching standard

	for _, t := range config.TurbineTags {
		events = append(events, model.SensorEvent{
			EventTime:   now,
			PIPointID:   t.PIPointID,
			TagName:     t.TagName,
			EquipmentID: t.Equipment,
			Value:       generateStatefulValue(pState, t),
			Quality:     0,
			Topic:       "turbine",
			Partition:   getPartition(t.Equipment),
		})
	}
	return events
}

func GenerateGeneratorEvents(pState *model.PlantState) []model.SensorEvent {
	var events []model.SensorEvent
	now := time.Now().UTC() // UTC matching standard

	for _, t := range config.GeneratorTags {
		events = append(events, model.SensorEvent{
			EventTime:   now,
			PIPointID:   t.PIPointID,
			TagName:     t.TagName,
			EquipmentID: t.Equipment,
			Value:       generateStatefulValue(pState, t),
			Quality:     0,
			Topic:       "generator",
			Partition:   getPartition(t.Equipment),
		})
	}
	return events
}

func GenerateCondenserEvents(pState *model.PlantState) []model.SensorEvent {
	var events []model.SensorEvent
	now := time.Now().UTC() // UTC matching standard

	for _, t := range config.CondenserTags {
		events = append(events, model.SensorEvent{
			EventTime:   now,
			PIPointID:   t.PIPointID,
			TagName:     t.TagName,
			EquipmentID: t.Equipment,
			Value:       generateStatefulValue(pState, t),
			Quality:     0,
			Topic:       "condenser",
			Partition:   getPartition(t.Equipment),
		})
	}
	return events
}

func GenerateCoolingTowerEvents(pState *model.PlantState) []model.SensorEvent {
	var events []model.SensorEvent
	now := time.Now().UTC() // UTC matching standard

	for _, t := range config.CoolingTowerTags {
		events = append(events, model.SensorEvent{
			EventTime:   now,
			PIPointID:   t.PIPointID,
			TagName:     t.TagName,
			EquipmentID: t.Equipment,
			Value:       generateStatefulValue(pState, t),
			Quality:     0,
			Topic:       "cooling-tower",
			Partition:   getPartition(t.Equipment),
		})
	}
	return events
}

func GenerateCoalHandlingEvents(pState *model.PlantState) []model.SensorEvent {
	var events []model.SensorEvent
	now := time.Now().UTC() // UTC matching standard

	for _, t := range config.CoalHandlingTags {
		events = append(events, model.SensorEvent{
			EventTime:   now,
			PIPointID:   t.PIPointID,
			TagName:     t.TagName,
			EquipmentID: t.Equipment,
			Value:       generateStatefulValue(pState, t),
			Quality:     0,
			Topic:       "coal-handling",
			Partition:   getPartition(t.Equipment),
		})
	}
	return events
}

func GenerateAshHandlingEvents(pState *model.PlantState) []model.SensorEvent {
	var events []model.SensorEvent
	now := time.Now().UTC() // UTC matching standard

	for _, t := range config.AshHandlingTags {
		events = append(events, model.SensorEvent{
			EventTime:   now,
			PIPointID:   t.PIPointID,
			TagName:     t.TagName,
			EquipmentID: t.Equipment,
			Value:       generateStatefulValue(pState, t),
			Quality:     0,
			Topic:       "ash-handling",
			Partition:   getPartition(t.Equipment),
		})
	}
	return events
}

func GenerateWaterTreatmentEvents(pState *model.PlantState) []model.SensorEvent {
	var events []model.SensorEvent
	now := time.Now().UTC() // UTC matching standard

	for _, t := range config.WaterTreatmentTags {
		events = append(events, model.SensorEvent{
			EventTime:   now,
			PIPointID:   t.PIPointID,
			TagName:     t.TagName,
			EquipmentID: t.Equipment,
			Value:       generateStatefulValue(pState, t),
			Quality:     0,
			Topic:       "water-treatment",
			Partition:   getPartition(t.Equipment),
		})
	}
	return events
}

func GenerateElectricalSystemEvents(pState *model.PlantState) []model.SensorEvent {
	var events []model.SensorEvent
	now := time.Now().UTC() // UTC matching standard

	for _, t := range config.ElectricalSystemTags {
		events = append(events, model.SensorEvent{
			EventTime:   now,
			PIPointID:   t.PIPointID,
			TagName:     t.TagName,
			EquipmentID: t.Equipment,
			Value:       generateStatefulValue(pState, t),
			Quality:     0,
			Topic:       "electrical-system",
			Partition:   getPartition(t.Equipment),
		})
	}
	return events
}

func GenerateInstrumentationControlEvents(pState *model.PlantState) []model.SensorEvent {
	var events []model.SensorEvent
	now := time.Now().UTC() // UTC matching standard

	for _, t := range config.InstrumentationControlTags {
		events = append(events, model.SensorEvent{
			EventTime:   now,
			PIPointID:   t.PIPointID,
			TagName:     t.TagName,
			EquipmentID: t.Equipment,
			Value:       generateStatefulValue(pState, t),
			Quality:     0,
			Topic:       "instrumentation-control",
			Partition:   getPartition(t.Equipment),
		})
	}
	return events
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
