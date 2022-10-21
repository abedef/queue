package queue

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	kafka "github.com/segmentio/kafka-go"
)

var QUEUE_ENABLED = false

var writer *kafka.Writer

// TODO: Gotta happen
// defer writer.Close()

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func getKafkaWriter() *kafka.Writer {
	url, ok := os.LookupEnv("KAFKA_URL")
	if !ok {
		log.Fatal("No value for environment variable KAFKA_URL")
	}
	topic, ok := os.LookupEnv("KAFKA_TOPIC")
	if !ok {
		log.Fatal("No value for environment variable KAFKA_TOPIC")
	}
	if writer == nil {
		writer = newKafkaWriter(url, topic)
	}
	return writer
}

func RecordEvent(key, value interface{}) error {
	// Let Kafka functionality be toggled until GCP deployment is figured out
	if !QUEUE_ENABLED {
		return nil
	}
	return getKafkaWriter().WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(fmt.Sprint(key)),
		Value: []byte(fmt.Sprint(value)),
	})
}
