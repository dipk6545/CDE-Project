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
    # Install Kafka binary in Codespaces if not present (kafka/ is in .gitignore)
    if [ ! -d "$KAFKA_DIR/libs" ]; then
        echo "📦 Downloading Apache Kafka with live progress bar..."
        mkdir -p "$KAFKA_DIR"
        curl -# -L https://downloads.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz | tar -xz -C "$KAFKA_DIR" --strip-components=1
        echo "✅ Kafka downloaded and extracted."
    fi

    # Reset any corrupted log directory and format cleanly
    rm -rf /tmp/kraft-combined-logs /tmp/kraft-broker-logs
    echo "🔧 Formatting Kafka KRaft storage..."
    CLUSTER_ID=$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)
    "$KAFKA_DIR/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$KAFKA_DIR/config/kraft/server.properties" > "$LOG_DIR/kafka_setup.log" 2>&1 || true

    echo "▶️ Launching Kafka server daemon in KRaft mode..."
    nohup "$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_DIR/config/kraft/server.properties" > "$LOG_DIR/kafka.log" 2>&1 &
    
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
echo "=========================================================="
echo ""
echo "📊 Launching live terminal stream visualizer..."
echo "👉 Press [Ctrl+C] at any time to return to terminal (streaming continues in background)"
echo ""
sleep 1
exec python3 scripts/monitor_stream.py
