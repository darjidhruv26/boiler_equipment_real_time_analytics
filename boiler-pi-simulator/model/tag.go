package model

type SensorEvent struct {
	EventTime   string      `json:"event_time"`
	PIPointID   int         `json:"pi_point_id"`
	TagName     string      `json:"tag_name"`
	EquipmentID string      `json:"equipment_id"`
	Value       interface{} `json:"value"`
	Quality     int         `json:"quality"`
	Topic       string      `json:"topic"`
	Partition   int         `json:"partition"`
}
