package simulator

import (
	"math/rand"
	"time"

	"boiler-pi-simulator/config"
	"boiler-pi-simulator/model"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func GenerateEvents() []model.TagEvent {

	var events []model.TagEvent

	now := time.Now()

	for _, tag := range config.BoilerTags {

		// simulate noise around base value
		noise := rand.Float64()*5 - 2.5

		value := tag.BaseValue + noise

		event := model.TagEvent{
			PIPointID: tag.PIPointID,
			Value:     value,
			Quality:   generateQuality(),
			Timestamp: now,
		}

		events = append(events, event)
	}

	return events
}

func generateQuality() uint8 {

	r := rand.Intn(100)

	switch {
	case r < 95:
		return 0 // GOOD
	case r < 98:
		return 2 // UNCERTAIN
	default:
		return 1 // BAD
	}
}
