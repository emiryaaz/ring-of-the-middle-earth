package kafka

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type EventConsumer struct {
	reader *kafka.Reader
}

func NewEventConsumer(broker string, topics []string, groupID string) *EventConsumer {
	return &EventConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{broker},
			GroupID:  groupID,
			GroupTopics: topics,
			MinBytes: 1,
			MaxBytes: 10e6,
		}),
	}
}

func (c *EventConsumer) Start(ctx context.Context, onMessage func(topic string, value []byte)) {
	go func() {
		for {
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Println("event consumer error:", err)
				continue
			}

			onMessage(msg.Topic, msg.Value)
		}
	}()
}

func (c *EventConsumer) Close() error {
	return c.reader.Close()
}
