package httpapi

import (
	"encoding/json"
	"net/http"

	"ring-of-the-middle-earth/option-b/internal/kafka"
	"ring-of-the-middle-earth/option-b/internal/model"
)

type OrderHandler struct {
	producer *kafka.Producer
}

func NewOrderHandler(producer *kafka.Producer) *OrderHandler {
	return &OrderHandler{producer: producer}
}

func (h *OrderHandler) HandleOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var order model.OrderSubmitted
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}

	data, err := json.Marshal(order)
	if err != nil {
		http.Error(w, "failed to marshal order", http.StatusInternalServerError)
		return
	}

	if err := h.producer.PublishOrder(r.Context(), order.PlayerID, data); err != nil {
		http.Error(w, "failed to publish order", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"accepted"}`))
}
