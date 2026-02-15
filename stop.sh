#!/bin/bash
# Stop Kafka cluster gracefully (data is preserved in Docker volumes)

echo "Stopping Kafka cluster..."
docker compose -f "$(dirname "$0")/docker-compose.yml" down
echo ""
echo "✅ Cluster stopped. Data is preserved in Docker volumes."
echo "   Run ./start.sh to bring it back up."
