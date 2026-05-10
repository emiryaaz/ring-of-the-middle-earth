package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	httpapi "ring-of-the-middle-earth/option-b/internal/http"
	"ring-of-the-middle-earth/option-b/internal/kafka"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	producer := kafka.NewProducer("localhost:9092")
	defer producer.Close()

	eventHub := httpapi.NewEventHub()
	go eventHub.Run()

	eventConsumer := kafka.NewEventConsumer(
		"localhost:9092",
		[]string{
			"game.events.unit",
			"game.events.path",
			"game.events.region",
			"game.broadcast",
			"game.ring.position",
			"game.ring.detection",
		},
		"go-event-router",
	)
	defer eventConsumer.Close()

	eventConsumer.Start(ctx, func(topic string, value []byte) {
		eventHub.Broadcast(fmt.Sprintf(`{"topic":"%s","value":%s}`, topic, string(value)))
	})

	orderHandler := httpapi.NewOrderHandler(producer)

	mux := http.NewServeMux()
	mux.HandleFunc("/order", orderHandler.HandleOrder)
	mux.HandleFunc("/events", eventHub.HandleEvents)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	log.Println("Go server running on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}
