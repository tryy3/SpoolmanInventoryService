package models

type EventType string

const (
	EventTypeInventory EventType = "inventory"
	EventTypePrinter   EventType = "printer"
)

type SpoolTransferReadyEvent struct {
	SpoolTransferInitiationEvent
	Type     EventType    `json:"type"`
	Filament Filament     `json:"filament"`
	Location Location     `json:"location"`
	Printer  *PrinterData `json:"printer,omitempty"`
}

type Filament struct {
	ID              int                 `json:"id"`
	Name            string              `json:"name"`
	Material        string              `json:"material"`
	Color           string              `json:"color"`
	Diameter        float64             `json:"diameter"`
	Density         float64             `json:"density"`
	RemainingWeight float64                 `json:"remainingWeight"`
	InitialWeight   float64                 `json:"initialWeight"`
	SpoolWeight     float64                 `json:"spoolWeight"`
	Temperature     FilamentTemperature `json:"temperature"`
}

type FilamentTemperature struct {
	Nozzle int `json:"nozzle"`
	Bed    int `json:"bed"`
}

type Location struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
