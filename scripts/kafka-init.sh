#!/bin/bash
set -e

# Start Kafka in the background
/etc/confluent/docker/run &
KAFKA_PID=$!

echo "Waiting for Kafka broker to be ready..."
until kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
  echo "Kafka not ready yet, waiting..."
  sleep 2
done

echo "Creating topic '$KAFKA_TOPIC' with $KAFKA_PARTITIONS partitions..."
kafka-topics --bootstrap-server localhost:9092 \
  --create \
  --topic $KAFKA_TOPIC \
  --partitions $KAFKA_PARTITIONS \
  --replication-factor 1 \
  --if-not-exists

echo "Verifying topic..."
kafka-topics --bootstrap-server localhost:9092 \
  --describe \
  --topic $KAFKA_TOPIC

echo "Kafka topic ready!"
wait $KAFKA_PID
