package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"boiler-pi-simulator/simulator"

	"github.com/segmentio/kafka-go"
)

const (
	kafkaBroker = "localhost:9092"
	topic       = "boiler-sensors"
)

func main() {

	// writer := kafka.NewWriter(kafka.WriterConfig{
	// 	Brokers:  []string{kafkaBroker},
	// 	Topic:    topic,
	// 	Balancer: &kafka.Hash{},
	// })
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic,
		Balancer: &kafka.Hash{},
	})
	fmt.Println("Connecting to Kafka:", kafkaBroker)
	fmt.Println("Boiler Kafka Producer Started")

	for {

		events := simulator.GenerateEvents()

		msgs := make([]kafka.Message, 0, len(events))

		for _, event := range events {
			msg, err := json.Marshal(event)
			if err != nil {
				fmt.Println("JSON error:", err)
				continue
			}
			fmt.Println(string(msg))
			msgs = append(msgs, kafka.Message{
				Key:   []byte(fmt.Sprint(event.PIPointID)),
				Value: msg,
			})
		}

		err := writer.WriteMessages(context.Background(), msgs...)
		if err != nil {
			fmt.Println("Kafka error:", err)
		} else {
			fmt.Printf("Sent batch of %d messages\n", len(msgs))
		}

		time.Sleep(1 * time.Second)
	}
}
