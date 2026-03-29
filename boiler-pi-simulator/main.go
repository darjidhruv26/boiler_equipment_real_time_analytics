package main

import (
	"fmt"
	"time"

	"boiler-pi-simulator/simulator"
)

func main() {
	fmt.Println("--- Simulator Manual Check ---")

	events := simulator.GenerateBoilerEvents() // []model.SensorEvent

	for _, event := range events {
		fmt.Printf("Tag: %d | Value: %.2f | Quality: %d | Time: %s\n",
			event.PIPointID, event.Value, event.Quality, event.EventTime.Format(time.RFC3339))
	}
}
