package models

type PrinterData struct {
	ID string `json:"id"`
	Name string `json:"name"`
	Endpoint string `json:"endpoint"`
	AMSSlot int `json:"amsSlot"`
	PrinterType string `json:"printerType"`
}