package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"boiler-pi-simulator/model"
	"boiler-pi-simulator/simulator"

	"github.com/segmentio/kafka-go"
)

const (
	boilerTopic                 = "boiler-sensors"
	turbineTopic                = "turbine"
	generatorTopic              = "generator"
	condenserTopic              = "condenser"
	coolingTowerTopic           = "cooling-tower"
	coalHandlingTopic           = "coal-handling"
	ashHandlingTopic            = "ash-handling"
	waterTreatmentTopic         = "water-treatment"
	electricalSystemTopic       = "electrical-system"
	instrumentationControlTopic = "instrumentation-control"
	gridCarbonTopic             = "grid-carbon-intensity"
)

var kafkaBroker = "localhost:9092"

// PartitionBalancer explicitly routes messages to the partition specified in the headers
type PartitionBalancer struct{}

func (b *PartitionBalancer) Balance(msg kafka.Message, partitions ...int) int {
	for _, h := range msg.Headers {
		if h.Key == "partition" {
			p, err := strconv.Atoi(string(h.Value))
			if err == nil {
				for _, avail := range partitions {
					if p == avail {
						return p // Return if partition maps to an available target
					}
				}
			}
		}
	}
	return 13 // Default partition map
}

// --- New State Management and Scenario Simulation ---

// plantState is a global, thread-safe struct holding the state of the entire plant.
var plantState = &model.PlantState{
	Equipment:           make(map[string]*model.EquipmentState),
	PlantLoad:           0.75, // Start at 75% load
	GridCarbonIntensity: 400,  // Baseline carbon intensity
}

// initializePlantState sets up the initial state for all major equipment groups.
func initializePlantState() {
	plantState.Lock()
	defer plantState.Unlock()

	// This list should correspond to the major equipment groups and their Kafka topics.
	equipmentGroups := []string{
		"BOILER", "TURBINE", "GENERATOR", "CONDENSER", "COOLING-TOWER",
		"COAL-HANDLING", "ASH-HANDLING", "WATER-TREATMENT", "ELECTRICAL-SYSTEM", "INSTRUMENTATION-CONTROL",
	}

	for _, group := range equipmentGroups {
		plantState.Equipment[group] = &model.EquipmentState{
			Name:           group,
			OperatingState: "RUNNING",
			Health:         1.0,
			Load:           1.0,
			SensorFailures: make(map[int]string),
		}
	}
	fmt.Println("Plant state initialized.")
}

// scenarioManager simulates real-world events to make the data more realistic.
// This is crucial for training AI agents for predictive maintenance and operational optimization.
func scenarioManager(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second) // Evaluate for a new scenario every 30 seconds
	defer ticker.Stop()

	fmt.Println("Scenario Manager Started: Will introduce dynamic events and failures.")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			plantState.Lock()
			// Randomly decide whether to introduce an event
			if rand.Float64() < 0.1 { // 10% chance each tick to cause an event
				switch rand.Intn(3) {
				case 0:
					// SCENARIO 1: Simulate a sensor failure for predictive maintenance agent
					fmt.Println("[SCENARIO] Simulating Condenser pressure sensor failure (stuck value).")
					if state, ok := plantState.Equipment["CONDENSER"]; ok {
						// Mark sensor 5001 as 'stuck'
						state.SensorFailures[5001] = "stuck"
					}
				case 1:
					// SCENARIO 2: Simulate equipment degradation (e.g., a leak)
					fmt.Println("[SCENARIO] Simulating gradual degradation of Cooling Tower efficiency.")
					if state, ok := plantState.Equipment["COOLING-TOWER"]; ok && state.Health > 0.7 {
						state.Health -= 0.05 // Reduce health by 5%
					}
				case 2:
					// SCENARIO 3: Simulate a non-critical load shed for VPP agent testing
					fmt.Println("[SCENARIO] Simulating demand response event: Temporarily shedding Ash Handling load.")
					if state, ok := plantState.Equipment["ASH-HANDLING"]; ok {
						state.OperatingState = "STANDBY"
						// Schedule it to come back online
						time.AfterFunc(5*time.Minute, func() {
							plantState.Lock()
							fmt.Println("[SCENARIO] Demand response event over. Resuming Ash Handling operations.")
							if s, ok := plantState.Equipment["ASH-HANDLING"]; ok {
								s.OperatingState = "RUNNING"
							}
							plantState.Unlock()
						})
					}
				}
			}
			plantState.Unlock()
		}
	}
}

// plantLoadManager simulates the daily fluctuation of the plant's power output.
func plantLoadManager(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	fmt.Println("Plant Load Manager Started: Simulating daily load changes.")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			plantState.Lock()
			// Simple simulation: ramp down at night, up during the day
			hour := time.Now().Hour()
			if hour > 22 || hour < 6 {
				plantState.PlantLoad = 0.5 // Night load
			} else {
				plantState.PlantLoad = 0.9 // Day load
			}
			plantState.Unlock()
		}
	}
}

// gridCarbonManager simulates the fluctuating carbon intensity of the power grid.
func gridCarbonManager(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	fmt.Println("Grid Carbon Manager Started: Simulating grid carbon intensity.")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			plantState.Lock()
			// Simple simulation: lower intensity during midday (solar)
			hour := time.Now().Hour()
			if hour > 10 && hour < 15 {
				plantState.GridCarbonIntensity = 250 + rand.Float64()*50 // Low carbon period
			} else {
				plantState.GridCarbonIntensity = 450 + rand.Float64()*50 // High carbon period
			}
			plantState.Unlock()
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Graceful shutdown handler
	go func() {
		<-sigChan
		fmt.Println("\nInterrupt signal received. Shutting down gracefully...")
		cancel()
	}()

	// Initialize the dynamic state of the plant
	initializePlantState()

	// Start background managers to simulate real-world conditions
	go scenarioManager(ctx)
	go plantLoadManager(ctx)
	go gridCarbonManager(ctx)

	// Allow overriding Kafka broker via environment variable for containerized environments
	if brokerEnv := os.Getenv("KAFKA_BROKER"); brokerEnv != "" {
		kafkaBroker = brokerEnv
	}

	fmt.Println("Connecting to Kafka:", kafkaBroker)

	// Define all producer jobs
	producerJobs := map[string]struct {
		generatorFunc func(*model.PlantState) []model.SensorEvent
		balancer      kafka.Balancer
	}{
		boilerTopic:                 {simulator.GenerateBoilerEvents, &kafka.Hash{}},
		turbineTopic:                {simulator.GenerateTurbineEvents, &PartitionBalancer{}},
		generatorTopic:              {simulator.GenerateGeneratorEvents, &PartitionBalancer{}},
		condenserTopic:              {simulator.GenerateCondenserEvents, &PartitionBalancer{}},
		coolingTowerTopic:           {simulator.GenerateCoolingTowerEvents, &PartitionBalancer{}},
		coalHandlingTopic:           {simulator.GenerateCoalHandlingEvents, &PartitionBalancer{}},
		ashHandlingTopic:            {simulator.GenerateAshHandlingEvents, &PartitionBalancer{}},
		waterTreatmentTopic:         {simulator.GenerateWaterTreatmentEvents, &PartitionBalancer{}},
		electricalSystemTopic:       {simulator.GenerateElectricalSystemEvents, &PartitionBalancer{}},
		instrumentationControlTopic: {simulator.GenerateInstrumentationControlEvents, &PartitionBalancer{}},
		gridCarbonTopic:             {simulator.GenerateGridCarbonEvents, &kafka.RoundRobin{}}, // New producer for grid data
	}

	var wg sync.WaitGroup
	wg.Add(len(producerJobs))

	for topic, job := range producerJobs {
		go runGenericProducer(ctx, &wg, topic, job.generatorFunc, job.balancer, plantState)
	}

	wg.Wait()
	fmt.Println("All producers stopped.")
}

// runGenericProducer is a refactored, robust function to handle all data production.
func runGenericProducer(ctx context.Context, wg *sync.WaitGroup, topic string, eventGenerator func(*model.PlantState) []model.SensorEvent, balancer kafka.Balancer, pState *model.PlantState) {
	defer wg.Done()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        topic,
		Balancer:     balancer,
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  10,
		BatchSize:    200,
		Compression:  kafka.Snappy,
	}
	defer writer.Close()

	fmt.Printf("Kafka Producer Started for topic: %s\n", topic)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events := eventGenerator(pState)
			msgs := make([]kafka.Message, 0, len(events))

			for _, event := range events {
				msgBytes, err := json.Marshal(event)
				if err != nil {
					fmt.Printf("JSON marshal error on topic %s: %v\n", topic, err)
					continue
				}

				msgs = append(msgs, kafka.Message{
					Key:   []byte(fmt.Sprint(event.PIPointID)),
					Value: msgBytes,
					Headers: []kafka.Header{
						{Key: "partition", Value: []byte(strconv.Itoa(event.Partition))},
					},
				})
			}

			if err := writer.WriteMessages(ctx, msgs...); err != nil {
				if ctx.Err() == nil {
					fmt.Printf("Kafka write error on topic %s: %v\n", topic, err)
				}
			} else {
				// This can be noisy, so it's commented out. Uncomment for debugging.
				// fmt.Printf("Sent batch of %d messages to topic %s\n", len(msgs), topic)
			}
		}
	}
}
