package kafka

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	return &Consumer{reader: reader}
}

// ReadMessage reads the next message from the topic.
// It blocks until a message is available or the context is cancelled.
func (c *Consumer) ReadMessage(ctx context.Context) (kafka.Message, error) {
	log.Println("Reading message from Kafka")
	return c.reader.ReadMessage(ctx)
}

// FetchMessage fetches the next message without committing.
// Use CommitMessages to commit after processing.
func (c *Consumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	log.Println("Fetching message from Kafka")
	return c.reader.FetchMessage(ctx)
}

// CommitMessages commits the given messages.
func (c *Consumer) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	log.Println("Committing messages to Kafka")
	return c.reader.CommitMessages(ctx, msgs...)
}

// Close closes the consumer connection.
func (c *Consumer) Close() error {
	log.Println("Closing consumer connection")
	return c.reader.Close()
}

