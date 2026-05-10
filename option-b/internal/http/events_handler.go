package httpapi

import (
	"fmt"
	"net/http"
)

type EventHub struct {
	clients    map[chan string]bool
	register   chan chan string
	unregister chan chan string
	broadcast  chan string
}

func NewEventHub() *EventHub {
	return &EventHub{
		clients:    make(map[chan string]bool),
		register:   make(chan chan string),
		unregister: make(chan chan string),
		broadcast:  make(chan string, 100),
	}
}

func (h *EventHub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true

		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client)
			}

		case message := <-h.broadcast:
			for client := range h.clients {
				select {
				case client <- message:
				default:
				}
			}
		}
	}
}

func (h *EventHub) Broadcast(message string) {
	h.broadcast <- message
}

func (h *EventHub) HandleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	client := make(chan string, 10)
	h.register <- client
	defer func() {
		h.unregister <- client
	}()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	for {
		select {
		case <-r.Context().Done():
			return

		case message := <-client:
			fmt.Fprintf(w, "data: %s\n\n", message)
			flusher.Flush()
		}
	}
}
