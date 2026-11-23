#!/usr/bin/env bash
# ==============================================================================
# ☸️ START AIRFLOW VIA DOCKER (Instant, 100% Reliable, 0 Timeouts)
# ==============================================================================
set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Pull latest changes
git pull origin main || true

# Load environment variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

echo "=========================================================="
echo "🚀 Starting Airflow via Docker (Instant Standalone Mode)..."
echo "=========================================================="

# Launch our custom image with Airflow + PySpark in 1 clean container
docker compose -f docker-compose-airflow.yml up -d

echo ""
echo -n "⏳ Waiting for Airflow UI on port 8080"
for i in {1..30}; do
    if nc -z localhost 8080 2>/dev/null; then
        echo " - Ready!"
        break
    fi
    echo -n "."
    sleep 2
done

echo ""
echo "=========================================================="
echo "🎉 AIRFLOW IS LIVE ON PORT 8080!"
echo "👉 Click 'Open in Browser' on the Port 8080 popup in Codespaces"
echo "🔑 Login: admin / admin"
echo "=========================================================="
