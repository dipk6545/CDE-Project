#!/usr/bin/env bash
# ==============================================================================
# 📡 1. START STREAMING & KAFKA INGESTION PIPELINE (via Docker Container)
# ==============================================================================
set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Pull latest changes from git
git pull origin main || true

# Load environment variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

LOG_DIR="$PROJECT_ROOT/logs_runtime"
mkdir -p "$LOG_DIR"

echo "=========================================================="
echo "🐳 Starting Apache Kafka via Docker (apache/kafka:3.8.0)..."
echo "=========================================================="

# Ensure Kafka docker container is running
if docker ps --format '{{.Names}}' | grep -q "^cde-kafka$"; then
    echo "✅ Kafka container 'cde-kafka' is already running."
else
    echo "▶️ Launching Kafka Docker container..."
    docker compose -f docker-compose-kafka.yml up -d
fi

echo -n "⏳ Waiting for Kafka broker on port 9092"
for i in {1..30}; do
    if nc -z localhost 9092 2>/dev/null; then
        echo " - Ready!"
        break
    fi
    echo -n "."
    sleep 1
done

echo ""
echo "=========================================================="
echo "📡 Starting Streaming Pipeline (Producer & Consumer)..."
echo "=========================================================="

PYTHON_BIN="$(which python3)"
if [ -f "/home/deep/anaconda3/envs/cde-py311/bin/python" ]; then
    PYTHON_BIN="/home/deep/anaconda3/envs/cde-py311/bin/python"
fi

# Ensure kafka-python-ng is installed
if ! "$PYTHON_BIN" -c "import kafka" 2>/dev/null; then
    echo "📦 Installing Python Kafka client..."
    pip install kafka-python-ng httpx boto3 >/dev/null 2>&1 || true
fi

# Start Consumer
if pgrep -f "streaming/consumer.py" > /dev/null; then
    echo "✅ Consumer is already running."
else
    echo "▶️ Launching Kafka Consumer (streaming/consumer.py)..."
    nohup "$PYTHON_BIN" streaming/consumer.py > "$LOG_DIR/consumer.log" 2>&1 &
fi

# Start Producer
if pgrep -f "streaming/producer.py" > /dev/null; then
    echo "✅ Producer is already running."
else
    echo "▶️ Launching Kafka Producer (streaming/producer.py)..."
    nohup "$PYTHON_BIN" streaming/producer.py > "$LOG_DIR/producer.log" 2>&1 &
fi

echo ""
echo "=========================================================="
echo "🎉 STREAMING INGESTION ACTIVE!"
echo "• Kafka:      Active in Docker on port 9092"
echo "• Producer:   Streaming live MBTA buses"
echo "• Consumer:   Syncing micro-batches to AWS S3 Bronze"
echo "=========================================================="
echo ""
echo "📊 Launching live terminal stream visualizer..."
echo "👉 Press [Ctrl+C] at any time to return to terminal (streaming continues in background)"
echo ""
sleep 1
exec "$PYTHON_BIN" scripts/monitor_stream.py
