#!/usr/bin/env bash
set -e

BROKER="localhost:9092"

docker exec kafka kafka-topics \
  --bootstrap-server "$BROKER" \
  --create --if-not-exists \
  --topic game.orders.raw \
  --partitions 3 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=3600000

docker exec kafka kafka-topics \
  --bootstrap-server "$BROKER" \
  --create --if-not-exists \
  --topic game.orders.validated \
  --partitions 6 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=3600000

docker exec kafka kafka-topics \
  --bootstrap-server "$BROKER" \
  --create --if-not-exists \
  --topic game.events.unit \
  --partitions 6 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000

docker exec kafka kafka-topics \
  --bootstrap-server "$BROKER" \
  --create --if-not-exists \
  --topic game.events.region \
  --partitions 6 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000

docker exec kafka kafka-topics \
  --bootstrap-server "$BROKER" \
  --create --if-not-exists \
  --topic game.events.path \
  --partitions 6 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000

docker exec kafka kafka-topics \
  --bootstrap-server "$BROKER" \
  --create --if-not-exists \
  --topic game.session \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact

docker exec kafka kafka-topics \
  --bootstrap-server "$BROKER" \
  --create --if-not-exists \
  --topic game.broadcast \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=3600000

docker exec kafka kafka-topics \
  --bootstrap-server "$BROKER" \
  --create --if-not-exists \
  --topic game.ring.position \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=3600000

docker exec kafka kafka-topics \
  --bootstrap-server "$BROKER" \
  --create --if-not-exists \
  --topic game.ring.detection \
  --partitions 2 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=3600000

docker exec kafka kafka-topics \
  --bootstrap-server "$BROKER" \
  --create --if-not-exists \
  --topic game.dlq \
  --partitions 3 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000

docker exec kafka kafka-topics --bootstrap-server "$BROKER" --list