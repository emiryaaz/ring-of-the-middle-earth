#!/usr/bin/env bash
set -e

SR_URL="http://localhost:8081"

register_schema() {
  local subject=$1
  local file=$2

  echo "Registering $subject from $file"
  curl -s -X POST "${SR_URL}/subjects/${subject}/versions" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "$(jq -Rs '{schema: .}' < "$file")"
  echo
  echo
}

register_schema "game.orders.raw-value" "kafka/schemas/order-submitted.avsc"
register_schema "game.orders.validated-value" "kafka/schemas/order-validated-v1.avsc"
register_schema "game.dlq-value" "kafka/schemas/dlq-entry.avsc"
register_schema "game.ring.position-value" "kafka/schemas/ring-event.avsc"
register_schema "game.ring.detection-value" "kafka/schemas/ring-event.avsc"
register_schema "game.broadcast-value" "kafka/schemas/broadcast-envelope.avsc"