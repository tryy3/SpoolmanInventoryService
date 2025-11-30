package models

type SpoolTransferCompleteEvent struct {
	SpoolTransferReadyEvent
	OldLocation Location `json:"oldLocation"`
}