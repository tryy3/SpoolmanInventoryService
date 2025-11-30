package config

import (
	"os"
	"strings"
)

type Config struct {
	KafkaBrokers      []string
	KafkaConsumerTopic string
	KafkaProducerTopic string
	KafkaConsumerGroup string
	SpoolmanAPIURL string
}

func Load() *Config {
	return &Config{
		KafkaBrokers:       getBrokers(),
		KafkaConsumerTopic: getEnv("KAFKA_CONSUMER_TOPIC", "3dprinter-filament-transfer-ready"),
		KafkaProducerTopic: getEnv("KAFKA_PRODUCER_TOPIC", "3dprinter-filament-transfer-completed"),
		KafkaConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "spoolman-inventory-service"),
		SpoolmanAPIURL: getEnv("SPOOLMAN_API_URL", "http://spoolman-svc.spoolman.svc.cluster.local/api/v1"),
	}
}

func getBrokers() []string {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	return strings.Split(brokers, ",")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

