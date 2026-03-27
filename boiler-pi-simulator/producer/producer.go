package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"boiler-pi-simulator/simulator"

	"github.com/segmentio/kafka-go"
)

const (
	kafkaBroker                 = "localhost:9092"
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
)

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

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nInterrupt signal received. Shutting down gracefully...")
		cancel()
	}()

	fmt.Println("Connecting to Kafka:", kafkaBroker)

	var wg sync.WaitGroup
	wg.Add(10)

	go runBoilerProducer(ctx, &wg)
	go runTurbineProducer(ctx, &wg)
	go runGeneratorProducer(ctx, &wg)
	go runCondenserProducer(ctx, &wg)
	go runCoolingTowerProducer(ctx, &wg)
	go runCoalHandlingProducer(ctx, &wg)
	go runAshHandlingProducer(ctx, &wg)
	go runWaterTreatmentProducer(ctx, &wg)
	go runElectricalSystemProducer(ctx, &wg)
	go runInstrumentationControlProducer(ctx, &wg)

	wg.Wait()
	fmt.Println("All producers stopped.")
}

func runBoilerProducer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        boilerTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  10,
		BatchSize:    200,
		Compression:  kafka.Snappy,
	}
	defer writer.Close()

	fmt.Println("Boiler Kafka Producer Started")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events := simulator.GenerateBoilerEvents()
			msgs := make([]kafka.Message, 0, len(events))

			for _, event := range events {
				msgBytes, err := json.Marshal(event)
				if err != nil {
					fmt.Println("Boiler JSON error:", err)
					continue
				}
				// fmt.Printf("[Boiler] Produced: %s\n", string(msgBytes))
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
					fmt.Println("Boiler Kafka error:", err)
				}
			} else {
				fmt.Printf("Sent batch of %d boiler messages\n", len(msgs))
			}
		}
	}
}

func runInstrumentationControlProducer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        instrumentationControlTopic,
		Balancer:     &PartitionBalancer{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  10,
		BatchSize:    200,
		Compression:  kafka.Snappy,
	}
	defer writer.Close()

	fmt.Println("Instrumentation & Control Kafka Producer Started")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events := simulator.GenerateInstrumentationControlEvents()
			msgs := make([]kafka.Message, 0, len(events))

			for _, event := range events {
				msgBytes, err := json.Marshal(event)
				if err != nil {
					fmt.Println("Instrumentation & Control JSON error:", err)
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
					fmt.Println("Instrumentation & Control Kafka error:", err)
				}
			} else {
				fmt.Printf("Sent batch of %d instrumentation & control messages\n", len(msgs))
			}
		}
	}
}

func runElectricalSystemProducer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        electricalSystemTopic,
		Balancer:     &PartitionBalancer{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  10,
		BatchSize:    200,
		Compression:  kafka.Snappy,
	}
	defer writer.Close()

	fmt.Println("Electrical System Kafka Producer Started")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events := simulator.GenerateElectricalSystemEvents()
			msgs := make([]kafka.Message, 0, len(events))

			for _, event := range events {
				msgBytes, err := json.Marshal(event)
				if err != nil {
					fmt.Println("Electrical System JSON error:", err)
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
					fmt.Println("Electrical System Kafka error:", err)
				}
			} else {
				fmt.Printf("Sent batch of %d electrical system messages\n", len(msgs))
			}
		}
	}
}

func runWaterTreatmentProducer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        waterTreatmentTopic,
		Balancer:     &PartitionBalancer{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  10,
		BatchSize:    200,
		Compression:  kafka.Snappy,
	}
	defer writer.Close()

	fmt.Println("Water Treatment Kafka Producer Started")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events := simulator.GenerateWaterTreatmentEvents()
			msgs := make([]kafka.Message, 0, len(events))

			for _, event := range events {
				msgBytes, err := json.Marshal(event)
				if err != nil {
					fmt.Println("Water Treatment JSON error:", err)
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
					fmt.Println("Water Treatment Kafka error:", err)
				}
			} else {
				fmt.Printf("Sent batch of %d water treatment messages\n", len(msgs))
			}
		}
	}
}

func runAshHandlingProducer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        ashHandlingTopic,
		Balancer:     &PartitionBalancer{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  10,
		BatchSize:    200,
		Compression:  kafka.Snappy,
	}
	defer writer.Close()

	fmt.Println("Ash Handling Kafka Producer Started")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events := simulator.GenerateAshHandlingEvents()
			msgs := make([]kafka.Message, 0, len(events))

			for _, event := range events {
				msgBytes, err := json.Marshal(event)
				if err != nil {
					fmt.Println("Ash Handling JSON error:", err)
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
					fmt.Println("Ash Handling Kafka error:", err)
				}
			} else {
				fmt.Printf("Sent batch of %d ash handling messages\n", len(msgs))
			}
		}
	}
}

func runCoalHandlingProducer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        coalHandlingTopic,
		Balancer:     &PartitionBalancer{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  10,
		BatchSize:    200,
		Compression:  kafka.Snappy,
	}
	defer writer.Close()

	fmt.Println("Coal Handling Kafka Producer Started")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events := simulator.GenerateCoalHandlingEvents()
			msgs := make([]kafka.Message, 0, len(events))

			for _, event := range events {
				msgBytes, err := json.Marshal(event)
				if err != nil {
					fmt.Println("Coal Handling JSON error:", err)
					continue
				}
				// fmt.Printf("[Coal Handling] Produced: %s\n", string(msgBytes)) // Uncomment to debug

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
					fmt.Println("Coal Handling Kafka error:", err)
				}
			} else {
				fmt.Printf("Sent batch of %d coal handling messages\n", len(msgs))
			}
		}
	}
}

func runCoolingTowerProducer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        coolingTowerTopic,
		Balancer:     &PartitionBalancer{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  10,
		BatchSize:    200,
		Compression:  kafka.Snappy,
	}
	defer writer.Close()

	fmt.Println("Cooling Tower Kafka Producer Started")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events := simulator.GenerateCoolingTowerEvents()
			msgs := make([]kafka.Message, 0, len(events))

			for _, event := range events {
				msgBytes, err := json.Marshal(event)
				if err != nil {
					fmt.Println("Cooling Tower JSON error:", err)
					continue
				}
				fmt.Printf("[Cooling Tower] Produced: %s\n", string(msgBytes))

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
					fmt.Println("Cooling Tower Kafka error:", err)
				}
			} else {
				fmt.Printf("Sent batch of %d cooling tower messages\n", len(msgs))
			}
		}
	}
}

func runCondenserProducer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        condenserTopic,
		Balancer:     &PartitionBalancer{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  10,
		BatchSize:    200,
		Compression:  kafka.Snappy,
	}
	defer writer.Close()

	fmt.Println("Condenser Kafka Producer Started")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events := simulator.GenerateCondenserEvents()
			msgs := make([]kafka.Message, 0, len(events))

			for _, event := range events {
				msgBytes, err := json.Marshal(event)
				if err != nil {
					fmt.Println("Condenser JSON error:", err)
					continue
				}
				// fmt.Printf("[Condenser] Produced: %s\n", string(msgBytes))

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
					fmt.Println("Condenser Kafka error:", err)
				}
			} else {
				fmt.Printf("Sent batch of %d condenser messages\n", len(msgs))
			}
		}
	}
}

func runGeneratorProducer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        generatorTopic,
		Balancer:     &PartitionBalancer{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  10,
		BatchSize:    200,
		Compression:  kafka.Snappy,
	}
	defer writer.Close()

	fmt.Println("Generator Kafka Producer Started")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events := simulator.GenerateGeneratorEvents()
			msgs := make([]kafka.Message, 0, len(events))

			for _, event := range events {
				msgBytes, err := json.Marshal(event)
				if err != nil {
					fmt.Println("Generator JSON error:", err)
					continue
				}
				// fmt.Printf("[Generator] Produced: %s\n", string(msgBytes))

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
					fmt.Println("Generator Kafka error:", err)
				}
			} else {
				fmt.Printf("Sent batch of %d generator messages\n", len(msgs))
			}
		}
	}
}

func runTurbineProducer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        turbineTopic,
		Balancer:     &PartitionBalancer{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  10,
		BatchSize:    200,
		Compression:  kafka.Snappy,
	}
	defer writer.Close()

	fmt.Println("Turbine Kafka Producer Started")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events := simulator.GenerateTurbineEvents()
			msgs := make([]kafka.Message, 0, len(events))

			for _, event := range events {
				msgBytes, err := json.Marshal(event)
				if err != nil {
					fmt.Println("Turbine JSON error:", err)
					continue
				}
				fmt.Printf("[Turbine] Produced: %s\n", string(msgBytes))

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
					fmt.Println("Turbine Kafka error:", err)
				}
			} else {
				fmt.Printf("Sent batch of %d turbine messages\n", len(msgs))
			}
		}
	}
}
