#!/usr/bin/env bash
# ==============================================================================
# 📡 1. START STREAMING & KAFKA INGESTION PIPELINE
# Starts:
#   - Kafka Broker (KRaft mode) on localhost:9092
#   - Kafka Consumer (writes bronze_stream.jsonl & syncs to AWS S3 Bronze)
#   - Kafka Producer (fetches live MBTA bus coordinates)
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
echo "🚀 Starting Apache Kafka (KRaft mode) in background..."
echo "=========================================================="

KAFKA_DIR="$PROJECT_ROOT/kafka"
if pgrep -f "kafka.Kafka" > /dev/null; then
    echo "✅ Kafka broker is already running."
else
    if [ ! -d "/tmp/kraft-broker-logs" ] && [ ! -d "/tmp/kraft-combined-logs" ]; then
        echo "🔧 Formatting Kafka KRaft storage..."
        bash "$KAFKA_DIR/setup_kraft.sh" > "$LOG_DIR/kafka_setup.log" 2>&1 || true
    fi

    echo "▶️ Launching Kafka server daemon..."
    nohup bash "$KAFKA_DIR/start_kafka.sh" > "$LOG_DIR/kafka.log" 2>&1 &
    
    echo -n "⏳ Waiting for Kafka broker on port 9092"
    for i in {1..30}; do
        if nc -z localhost 9092 2>/dev/null || (exec 3<>/dev/tcp/localhost/9092) 2>/dev/null; then
            echo " - Ready!"
            break
        fi
        echo -n "."
        sleep 1
    done
fi

echo ""
echo "=========================================================="
echo "📡 Starting Streaming Pipeline (Producer & Consumer)..."
echo "=========================================================="

PYTHON_BIN="$(which python3)"
if [ -f "/home/deep/anaconda3/envs/cde-py311/bin/python" ]; then
    PYTHON_BIN="/home/deep/anaconda3/envs/cde-py311/bin/python"
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
echo "• Kafka:      Active on localhost:9092"
echo "• Producer:   Fetching live MBTA vehicles"
echo "• Consumer:   Syncing micro-batches to AWS S3 Bronze"
echo "• Logs:       tail -f logs_runtime/consumer.log"
echo "=========================================================="
