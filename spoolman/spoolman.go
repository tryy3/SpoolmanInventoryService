package spoolman

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/tryy3/SpoolmanInventoryService/models"
)

type SpoolmanSettingsValue struct {
	Value string `json:"value"`
	IsSet bool `json:"is_set"`
	Type string `json:"type"`
}

type SpoolmanClient struct {
	APIURL string
}

func NewSpoolmanClient(apiURL string) *SpoolmanClient {
	return &SpoolmanClient{
		APIURL: apiURL,
	}
}

func (c *SpoolmanClient) GetInventories() ([]string, error) {
	response, err := http.Get(fmt.Sprintf("%s/setting/locations", c.APIURL))
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	log.Printf("Spoolman API response for settings values: %s", string(body))

	var settingsValues SpoolmanSettingsValue
	if err := json.Unmarshal(body, &settingsValues); err != nil {
		return nil, err
	}

	var inventories []string
	if err := json.Unmarshal([]byte(settingsValues.Value), &inventories); err != nil {
		return nil, err
	}
	log.Printf("Got inventories: %+v", inventories)
	return inventories, nil
}

func (c *SpoolmanClient) GetSpoolData(spoolId string) (models.SpoolmanSpoolData, error) {
	response, err := http.Get(fmt.Sprintf("%s/spool/%s", c.APIURL, spoolId))
	if err != nil {
		return models.SpoolmanSpoolData{}, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return models.SpoolmanSpoolData{}, err
	}
	log.Printf("Spoolman API response: %s", string(body))
	var spoolData models.SpoolmanSpoolData
	if err := json.Unmarshal(body, &spoolData); err != nil {
		return models.SpoolmanSpoolData{}, err
	}

	return spoolData, nil
}

func (c *SpoolmanClient) UpdateSpoolInventory(spoolId string, inventoryId string) error {
	data, err := json.Marshal(map[string]string{
		"location": inventoryId,
	})
	if err != nil {
		return err
	}
	request, err := http.NewRequest("PATCH", fmt.Sprintf("%s/spool/%s", c.APIURL, spoolId), bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	return nil	
}