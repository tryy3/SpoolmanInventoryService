package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tryy3/SpoolmanInventoryService/config"
	"github.com/tryy3/SpoolmanInventoryService/kafka"
	"github.com/tryy3/SpoolmanInventoryService/models"
	"github.com/tryy3/SpoolmanInventoryService/spoolman"
)

func main() {
	cfg := config.Load()

	log.Printf("Starting Spoolman Inventory Service")
	log.Printf("Kafka Brokers: %v", cfg.KafkaBrokers)
	log.Printf("Consumer Topic: %s", cfg.KafkaConsumerTopic)
	log.Printf("Producer Topic: %s", cfg.KafkaProducerTopic)
	log.Printf("Consumer Group: %s", cfg.KafkaConsumerGroup)

	// Create consumer and producer
	consumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaConsumerTopic, cfg.KafkaConsumerGroup)
	defer consumer.Close()

	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaProducerTopic)
	defer producer.Close()

	spoolmanClient := spoolman.NewSpoolmanClient(cfg.SpoolmanAPIURL)

	// Setup context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutdown signal received, stopping...")
		cancel()
	}()

	// Start consuming messages
	log.Println("Starting to consume messages...")
	for {
		msg, err := consumer.FetchMessage(ctx)
		log.Println("Fetched message from Kafka")
		if err != nil {
			if ctx.Err() != nil {
				// Context cancelled, shutting down
				break
			}
			log.Printf("Error fetching message: %v", err)
			continue
		}

		log.Printf("Received message: key=%s value=%s", string(msg.Key), string(msg.Value))

		var spoolTransferReadyEvent models.SpoolTransferReadyEvent
		if err := json.Unmarshal(msg.Value, &spoolTransferReadyEvent); err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			if err := consumer.CommitMessages(ctx, msg); err != nil {
				log.Printf("Error committing message: %v", err)
			}
			continue
		}
		log.Printf("Unmarshalled message: %+v", spoolTransferReadyEvent)

		inventories, err := spoolmanClient.GetInventories()
		if err != nil {
			log.Printf("Error getting inventories: %v", err)
			if err := consumer.CommitMessages(ctx, msg); err != nil {
				log.Printf("Error committing message: %v", err)
			}
			continue
		}
		log.Printf("Got inventories: %+v", inventories)

		// Check if the inventory from the event is actually a valid inventory
		validInventory := false
		for _, inventory := range inventories {
			if inventory == spoolTransferReadyEvent.LocationId {
				validInventory = true
				break
			}
		}
		if !validInventory {
			log.Printf("Invalid inventory: %s", spoolTransferReadyEvent.LocationId)
			if err := consumer.CommitMessages(ctx, msg); err != nil {
				log.Printf("Error committing message: %v", err)
			}
			continue
		}

		// Get spool data for the spool in the event
		spoolData, err := spoolmanClient.GetSpoolData(spoolTransferReadyEvent.SpoolId)
		if err != nil {
			log.Printf("Error getting spool data: %v", err)
			if err := consumer.CommitMessages(ctx, msg); err != nil {
				log.Printf("Error committing message: %v", err)
			}
			continue
		}
		log.Printf("Got spool data: %+v", spoolData)

		err = spoolmanClient.UpdateSpoolInventory(spoolTransferReadyEvent.SpoolId, spoolTransferReadyEvent.LocationId)
		if err != nil {
			log.Printf("Error updating spool inventory: %v", err)
			if err := consumer.CommitMessages(ctx, msg); err != nil {
				log.Printf("Error committing message: %v", err)
			}
			continue
		}

		// Get the old location data
		oldLocation := buildLocation(spoolData.LocationId)

		completeEvent := models.SpoolTransferCompleteEvent{
			SpoolTransferReadyEvent: spoolTransferReadyEvent,
			OldLocation: oldLocation,
		}

		outputValue, err := json.Marshal(completeEvent)
		if err != nil {
			log.Printf("Error marshalling complete event: %v", err)
			continue
		}

		if err := producer.WriteMessage(ctx, msg.Key, outputValue); err != nil {
			log.Printf("Error producing message: %v", err)
			continue
		}

		if err := consumer.CommitMessages(ctx, msg); err != nil {	
			log.Printf("Error committing message: %v", err)
		}

		log.Printf("Successfully processed and forwarded message")
	}

	log.Println("Service stopped")
}

func buildLocation(locationId string) models.Location {
	return models.Location{
		ID:   locationId,
		Name: locationId, // TODO: Look up actual location name if available
	}
}