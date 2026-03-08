package model

import "time"

type TagEvent struct {
	PIPointID uint32    `json:"pi_point_id"`
	Value     float64   `json:"value"`
	Quality   uint8     `json:"quality"`
	Timestamp time.Time `json:"timestamp"`
}
