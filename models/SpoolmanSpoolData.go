package models

type SpoolmanSpoolData struct {
	Id int `json:"id"`
	RemainingWeight float64 `json:"remaining_weight"`
	InitialWeight float64 `json:"initial_weight"`
	SpoolWeight float64 `json:"spool_weight"`
	UsedWeight float64 `json:"used_weight"`
	RemainingLength float64 `json:"remaining_length"`
	UsedLength float64 `json:"used_length"`
	LocationId string `json:"location"`
	Filament SpoolmanFilamentData `json:"filament"`
}

type SpoolmanFilamentData struct {
	Id int `json:"id"`
	Name string `json:"name"`
	Material string `json:"material"`
	Density float64 `json:"density"`
	Diameter float64 `json:"diameter"`
	Weight float64 `json:"weight"`
	SpoolWeight float64 `json:"spool_weight"`
	ExtruderTemperature int `json:"settings_extruder_temp"`
	BedTemperature int `json:"settings_bed_temp"`
	ColorHex string `json:"color_hex"`
	Vendor SpoolmanVendorData `json:"vendor"`
}

type SpoolmanVendorData struct {
	Id int `json:"id"`
	Name string `json:"name"`
}