package httpapi

import (
	"fmt"
	"net/http"
)

type EventHub struct {
	lightClients map[chan string]bool
	darkClients  map[chan string]bool

	registerLight   chan chan string
	registerDark    chan chan string
	unregisterLight chan chan string
	unregisterDark  chan chan string

	broadcastLight chan string
	broadcastDark  chan string
	broadcastBoth  chan string
}

func NewEventHub() *EventHub {
	return &EventHub{
		lightClients: make(map[chan string]bool),
		darkClients:  make(map[chan string]bool),

		registerLight:   make(chan chan string),
		registerDark:    make(chan chan string),
		unregisterLight: make(chan chan string),
		unregisterDark:  make(chan chan string),

		broadcastLight: make(chan string, 100),
		broadcastDark:  make(chan string, 100),
		broadcastBoth:  make(chan string, 100),
	}
}
func (h *EventHub) Run() {
	for {
		select {
		case client := <-h.registerLight:
			h.lightClients[client] = true

		case client := <-h.registerDark:
			h.darkClients[client] = true

		case client := <-h.unregisterLight:
			if _, ok := h.lightClients[client]; ok {
				delete(h.lightClients, client)
				close(client)
			}

		case client := <-h.unregisterDark:
			if _, ok := h.darkClients[client]; ok {
				delete(h.darkClients, client)
				close(client)
			}

		case message := <-h.broadcastLight:
			h.sendTo(h.lightClients, message)

		case message := <-h.broadcastDark:
			h.sendTo(h.darkClients, message)

		case message := <-h.broadcastBoth:
			h.sendTo(h.lightClients, message)
			h.sendTo(h.darkClients, message)
		}
	}
}

func (h *EventHub) sendTo(clients map[chan string]bool, message string) {
	for client := range clients {
		select {
		case client <- message:
		default:
		}
	}
}

func (h *EventHub) BroadcastLight(message string) {
	h.broadcastLight <- message
}

func (h *EventHub) BroadcastDark(message string) {
	h.broadcastDark <- message
}

func (h *EventHub) BroadcastBoth(message string) {
	h.broadcastBoth <- message
}

func (h *EventHub) HandleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	side := r.URL.Query().Get("side")
	if side != "light" && side != "dark" {
		http.Error(w, "side must be light or dark", http.StatusBadRequest)
		return
	}

	client := make(chan string, 10)

	if side == "light" {
		h.registerLight <- client
		defer func() {
			h.unregisterLight <- client
		}()
	} else {
		h.registerDark <- client
		defer func() {
			h.unregisterDark <- client
		}()
	}

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

