package models

type SpoolTransferInitiationEvent struct {
	SpoolId string `json:"spoolId"`
	LocationId string `json:"locationId"`
	Timestamp string `json:"timestamp"`
	TagData FilamentTagData `json:"tagData"`
}

type FilamentTagData struct {
	Id string `json:"id"`
	Name string `json:"name"`
	Material string `json:"material"`
	Color string `json:"color"`
	Temperature TemperatureInfo `json:"temperature"`
	Diameter float64 `json:"diameter"`
	Weight int `json:"weight"`
	SpoolWeight int `json:"spoolWeight"`
}

type TemperatureInfo struct {
	Nozzle int `json:"nozzle"`
	Bed int `json:"bed"`
}