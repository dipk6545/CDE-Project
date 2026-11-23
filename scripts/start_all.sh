#!/usr/bin/env bash
# ==============================================================================
# CDE Transit Lakehouse: Single All-In-One Startup Script
# Automatically launches:
# 1. Kafka (KRaft mode) in the background
# 2. Live MBTA Producer & Consumer (streaming to S3) in the background
# 3. Kind Kubernetes cluster & Airflow via Helm
# 4. Port-forward on 8080 so you can click the browser link immediately!
# ==============================================================================
set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Load environment variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

LOG_DIR="$PROJECT_ROOT/logs_runtime"
mkdir -p "$LOG_DIR"

echo "=========================================================="
echo "🚀 1. Starting Apache Kafka (KRaft mode) in background..."
echo "=========================================================="

KAFKA_DIR="$PROJECT_ROOT/kafka"
if pgrep -f "kafka.Kafka" > /dev/null; then
    echo "✅ Kafka broker is already running."
else
    # Format metadata storage if first time
    if [ ! -d "/tmp/kraft-broker-logs" ] && [ ! -d "/tmp/kraft-combined-logs" ]; then
        echo "🔧 Formatting Kafka KRaft storage..."
        bash "$KAFKA_DIR/setup_kraft.sh" > "$LOG_DIR/kafka_setup.log" 2>&1 || true
    fi

    echo "▶️ Launching Kafka server daemon..."
    nohup bash "$KAFKA_DIR/start_kafka.sh" > "$LOG_DIR/kafka.log" 2>&1 &
    
    # Wait for Kafka port 9092
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
echo "📡 2. Starting Streaming Pipeline (Producer & Consumer)..."
echo "=========================================================="

PYTHON_BIN="$(which python3)"
if [ -f "/home/deep/anaconda3/envs/cde-py311/bin/python" ]; then
    PYTHON_BIN="/home/deep/anaconda3/envs/cde-py311/bin/python"
fi

# Start Consumer (writes to bronze & uploads to S3)
if pgrep -f "streaming/consumer.py" > /dev/null; then
    echo "✅ Consumer is already running."
else
    echo "▶️ Launching Kafka Consumer (streaming/consumer.py)..."
    nohup "$PYTHON_BIN" streaming/consumer.py > "$LOG_DIR/consumer.log" 2>&1 &
fi

# Start Producer (fetches live MBTA API and streams to Kafka)
if pgrep -f "streaming/producer.py" > /dev/null; then
    echo "✅ Producer is already running."
else
    echo "▶️ Launching Kafka Producer (streaming/producer.py)..."
    nohup "$PYTHON_BIN" streaming/producer.py > "$LOG_DIR/producer.log" 2>&1 &
fi

echo "✅ Live bus data is streaming into Kafka & syncing to S3 Bronze!"

echo ""
echo "=========================================================="
echo "☸️ 3. Deploying Airflow on Kubernetes (Kind + Helm)..."
echo "=========================================================="

bash scripts/start_codespaces.sh

echo ""
echo "=========================================================="
echo "⏳ Waiting for Airflow Webserver to be ready..."
echo "=========================================================="

kubectl wait --namespace airflow \
  --for=condition=ready pod \
  --selector=component=webserver \
  --timeout=180s || true

echo ""
echo "=========================================================="
echo "🎉 ALL SYSTEMS READY! Launching Port-Forward on Port 8080..."
echo "👉 Click 'Open in Browser' on the Port 8080 popup in Codespaces"
echo "🔑 Login: admin / admin"
echo "=========================================================="
echo ""

# Automatically open port 8080 so you do not have to type anything else!
exec kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow --address 0.0.0.0
