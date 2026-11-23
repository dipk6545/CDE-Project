#!/usr/bin/env bash
# ==============================================================================
# 📡 1. START STREAMING & KAFKA INGESTION PIPELINE (Bulletproof)
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

# If kafka binaries missing, download 4.4.0
if [ ! -d "$KAFKA_DIR/libs" ] || [ $(ls -1 "$KAFKA_DIR/libs"/*.jar 2>/dev/null | wc -l) -lt 10 ]; then
    echo "📦 Downloading Apache Kafka 4.4.0..."
    rm -rf "$KAFKA_DIR"
    mkdir -p "$KAFKA_DIR"
    curl -# -L https://downloads.apache.org/kafka/4.4.0/kafka_2.13-4.4.0.tgz | tar -xz -C "$KAFKA_DIR" --strip-components=1
    echo "✅ Kafka downloaded and extracted."
fi

# Ensure classpath uses java wildcard
KAFKA_CP="$KAFKA_DIR/libs/*"

if nc -z localhost 9092 2>/dev/null; then
    echo "✅ Kafka broker is already running on port 9092."
else
    # Format metadata storage if not formatted
    if [ ! -f "/tmp/kraft-combined-logs/meta.properties" ]; then
        echo "🔧 Formatting Kafka KRaft storage..."
        rm -rf /tmp/kraft-combined-logs /tmp/kraft-broker-logs
        CLUSTER_ID=$(java -cp "$KAFKA_CP" kafka.tools.StorageTool random-uuid)
        java -cp "$KAFKA_CP" kafka.tools.StorageTool format --standalone -t "$CLUSTER_ID" -c "$KAFKA_DIR/config/server.properties" > "$LOG_DIR/kafka_setup.log" 2>&1
        echo "✅ Storage formatted successfully."
    fi

    echo "▶️ Launching Kafka server daemon..."
    nohup java -Xmx512M -cp "$KAFKA_CP" kafka.Kafka "$KAFKA_DIR/config/server.properties" > "$LOG_DIR/kafka.log" 2>&1 &
    
    echo -n "⏳ Waiting for Kafka broker on port 9092"
    for i in {1..30}; do
        if nc -z localhost 9092 2>/dev/null; then
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

# Select best python binary with kafka support
PYTHON_BIN="$(which python3)"
if [ -f "/home/deep/anaconda3/envs/cde-py311/bin/python" ]; then
    PYTHON_BIN="/home/deep/anaconda3/envs/cde-py311/bin/python"
fi

# Ensure kafka-python-ng or kafka-python is available
if ! "$PYTHON_BIN" -c "import kafka" 2>/dev/null; then
    echo "📦 Installing kafka client library..."
    pip install kafka-python-ng httpx boto3 2>/dev/null || true
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
exec "$PYTHON_BIN" scripts/monitor_stream.py
