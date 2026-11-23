#!/usr/bin/env bash
set -e

echo "=========================================================="
echo "🚀 Starting CDE Transit Lakehouse on Kubernetes in Codespaces"
echo "=========================================================="

# Load .env if present
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# 1. Install Kind if missing
if ! command -v kind &> /dev/null; then
    echo "📦 Installing Kind..."
    curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.24.0/kind-linux-amd64
    chmod +x ./kind && sudo mv ./kind /usr/local/bin/kind
    echo "✅ Kind installed."
else
    echo "✅ Kind is already installed."
fi

# 2. Install Helm if missing
if ! command -v helm &> /dev/null; then
    echo "📦 Installing Helm..."
    curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
    echo "✅ Helm installed."
else
    echo "✅ Helm is already installed."
fi

# 3. Add & update Airflow Helm repo
echo "📦 Updating Airflow Helm repo..."
helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
helm repo update apache-airflow

# 4. Create Kind Kubernetes cluster if not already running
if ! kind get clusters 2>/dev/null | grep -q "^cde-cluster$"; then
    echo "☸️ Creating Kind cluster 'cde-cluster'..."
    kind create cluster --name cde-cluster
    echo "✅ Cluster created."
else
    echo "✅ Kind cluster 'cde-cluster' already running."
fi

# 5. Create airflow namespace
kubectl create namespace airflow --dry-run=client -o yaml | kubectl apply -f -

# 6. Inject AWS and CDE secrets (safely read from environment / .env)
echo "🔐 Injecting AWS and Environment Secrets..."
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

# 7. Check if Airflow is already deployed via Helm
if helm status airflow -n airflow >/dev/null 2>&1; then
    echo "✅ Airflow Helm release is already installed and running. Skipping Helm deployment!"
else
    echo "🚢 Deploying Airflow via Helm (first time setup)..."
    helm upgrade --install airflow apache-airflow/airflow \
      --namespace airflow \
      --version 1.16.0 \
      -f orchestration/k8s-values.yaml
fi

echo ""
echo "=========================================================="
echo "🎉 DEPLOYMENT COMPLETE! All pods are starting up."
echo "=========================================================="
echo ""
echo "To expose the Airflow UI on Port 8080, run:"
echo "👉 kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow --address 0.0.0.0"
echo ""
