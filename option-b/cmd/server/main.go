package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	httpapi "ring-of-the-middle-earth/option-b/internal/http"
	"ring-of-the-middle-earth/option-b/internal/kafka"
)

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:5500")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

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
	message := fmt.Sprintf(`{"topic":"%s","value":%s}`, topic, string(value))

	switch topic {
	case "game.ring.position":
		eventHub.BroadcastLight(message)

	case "game.ring.detection":
		eventHub.BroadcastDark(message)

	default:
		eventHub.BroadcastBoth(message)
	}
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
	if err := http.ListenAndServe(":8080", withCORS(mux)); err != nil {
		log.Fatal(err)
	}
}
