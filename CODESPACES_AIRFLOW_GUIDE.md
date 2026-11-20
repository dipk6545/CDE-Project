# 🚀 How to Run Airflow on Kubernetes in GitHub Codespaces

A complete, copy-paste guide to spin up the entire **Kubernetes + Airflow + PySpark + Amazon S3** lakehouse pipeline inside GitHub Codespaces in under 2 minutes.

---

## 📋 Prerequisites Check (Inside Codespaces)
When you first open a new GitHub Codespace on this repo, run this single block to install `kind` and `helm`:

```bash
# 1. Install Kind
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.24.0/kind-linux-amd64
chmod +x ./kind && sudo mv ./kind /usr/local/bin/kind

# 2. Install Helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# 3. Add Airflow Helm Repository
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

---

## 🛠️ Step 1: Create the Local Kubernetes Cluster

```bash
# 1. Create the Kind cluster
kind create cluster --name cde-cluster

# 2. Create the airflow namespace
kubectl create namespace airflow
```

---

## 🔐 Step 2: Inject AWS & Environment Secrets

Read credentials securely from your `.env` file and create the Kubernetes Secret:

```bash
# Read keys from your .env file
source .env 2>/dev/null

kubectl create secret generic cde-aws-secrets -n airflow \
  --from-literal=AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
  --from-literal=AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
  --from-literal=AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
  --from-literal=AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-southeast-2}" \
  --from-literal=AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-southeast-2}" \
  --from-literal=CDE_ENVIRONMENT="cloud" \
  --from-literal=AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__CDE_ENVIRONMENT="cloud" \
  --dry-run=client -o yaml | kubectl apply -f -
```

---

## 🚢 Step 3: Deploy Airflow via Helm

Deploy the verified configuration using our published production image (`ghcr.io/dipk6545/cde-airflow:latest`):

```bash
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  --version 1.16.0 \
  -f orchestration/k8s-values.yaml
```

Check that all pods turn `Running` (takes ~45 seconds):
```bash
kubectl get pods -n airflow -w
```

---

## 🌐 Step 4: Open the Airflow Web UI

Expose port 8080 to your browser:

```bash
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow --address 0.0.0.0
```

1. Go to the **Ports** tab in Codespaces.
2. Click the **Globe icon (Open in Browser)** on port 8080.
3. Login credentials:
   - **Username:** `admin`
   - **Password:** `admin`

---

## ⚡ Step 5: Trigger the Pipeline

1. In the Airflow UI, find: **`mbta_transit_lakehouse_pipeline`**.
2. Toggle the switch to **Active / Unpaused**.
3. Click the **Play button ▶️ (Trigger DAG)**.
4. Click on the DAG name and open the **Graph** view to watch all 4 tasks turn 🟩 **Green**:
   - `check_bronze_data_available` 🟩
   - `run_pyspark_silver_transformation` 🟩
   - `run_pyspark_gold_aggregations` 🟩
   - `pipeline_complete` 🟩

---

## 📊 Step 6: Verify Clean Parquet in Amazon S3

Run this in your terminal to see the new Parquet files created in Sydney:

```bash
python3 -c "
import boto3
s3 = boto3.client('s3', region_name='ap-southeast-2')
bucket = 'cde-transit-lakehouse-221950623036-ap-southeast-2'
resp = s3.list_objects_v2(Bucket=bucket, Prefix='gold/')
for obj in resp.get('Contents', []):
    if obj['Key'].endswith('.parquet'):
        print(' -', obj['Key'], f'({obj[\"Size\"]} bytes)')
"
```

---

## 🛑 How to Stop or Teardown

To shut down the cluster and clean up all resources in Codespaces:
```bash
kind delete cluster --name cde-cluster
```
