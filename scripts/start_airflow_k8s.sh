#!/usr/bin/env bash
# ==============================================================================
# ☸️ START AIRFLOW ON KUBERNETES & HELM (Local Machine / Production)
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
echo "☸️ Launching Airflow on Kubernetes (Kind Cluster + Helm)..."
echo "=========================================================="

# 0. Install Kind & Helm if missing in this environment
if ! command -v kind &> /dev/null; then
    echo "📦 Installing Kind..."
    curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.24.0/kind-linux-amd64
    chmod +x ./kind && sudo mv ./kind /usr/local/bin/kind
    echo "✅ Kind installed."
fi

if ! command -v helm &> /dev/null; then
    echo "📦 Installing Helm..."
    curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
    echo "✅ Helm installed."
fi

# 1. Start / Verify Kind cluster
if ! kind get clusters 2>/dev/null | grep -q "^cde-cluster$"; then
    echo "☸️ Creating Kind cluster 'cde-cluster'..."
    kind create cluster --name cde-cluster
    echo "✅ Cluster created."
else
    echo "✅ Kind cluster 'cde-cluster' already running."
fi

# 2. Namespace & Secrets
kubectl create namespace airflow --dry-run=client -o yaml | kubectl apply -f -

AWS_KEY="${AWS_ACCESS_KEY_ID:-}"
AWS_SEC="${AWS_SECRET_ACCESS_KEY:-}"
AWS_REG="${AWS_DEFAULT_REGION:-ap-southeast-2}"

kubectl create secret generic cde-aws-secrets -n airflow \
  --from-literal=AWS_ACCESS_KEY_ID="${AWS_KEY}" \
  --from-literal=AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__AWS_ACCESS_KEY_ID="${AWS_KEY}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${AWS_SEC}" \
  --from-literal=AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__AWS_SECRET_ACCESS_KEY="${AWS_SEC}" \
  --from-literal=AWS_DEFAULT_REGION="${AWS_REG}" \
  --from-literal=AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__AWS_DEFAULT_REGION="${AWS_REG}" \
  --from-literal=CDE_ENVIRONMENT="cloud" \
  --from-literal=AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__CDE_ENVIRONMENT="cloud" \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. Deploy Helm
echo "🚢 Deploying / Verifying Airflow Helm Release..."
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  --version 1.16.0 \
  -f orchestration/k8s-values.yaml

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
echo "🎉 AIRFLOW IS READY! Exposing Port 8080..."
echo "👉 Open http://localhost:8080 in your browser"
echo "🔑 Login: admin / admin"
echo "=========================================================="
echo ""

exec kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow --address 0.0.0.0
