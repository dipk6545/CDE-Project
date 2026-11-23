#!/usr/bin/env bash
# ==============================================================================
# ☸️ 2. START AIRFLOW ORCHESTRATION ON KUBERNETES
# Starts:
#   - Kind cluster & Airflow Helm (skips if already running!)
#   - Exposes Port 8080 automatically
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

echo "=========================================================="
echo "☸️ Launching Airflow Orchestration on Kubernetes..."
echo "=========================================================="

# Check and deploy Kind / Helm only if not already installed
bash scripts/start_codespaces.sh

echo ""
echo "=========================================================="
echo "⏳ Ensuring Airflow Webserver is ready..."
echo "=========================================================="

kubectl wait --namespace airflow \
  --for=condition=ready pod \
  --selector=component=webserver \
  --timeout=60s || true

echo ""
echo "=========================================================="
echo "🎉 AIRFLOW IS READY! Exposing Port 8080..."
echo "👉 Click 'Open in Browser' on the Port 8080 popup in Codespaces"
echo "🔑 Login: admin / admin"
echo "=========================================================="
echo ""

exec kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow --address 0.0.0.0
