#!/bin/bash
# Start Kafka cluster (picks up existing data from Docker volumes)

echo "Starting Kafka cluster..."
docker compose -f "$(dirname "$0")/docker-compose.yml" up -d

echo ""
echo "Waiting for brokers to be ready..."
sleep 5

docker compose -f "$(dirname "$0")/docker-compose.yml" ps
echo ""
echo "✅ Cluster is up."
echo "   Kafka broker 1: localhost:9092"
echo "   Kafka broker 2: localhost:9094"
echo "   Kafka UI:       http://localhost:8080"
