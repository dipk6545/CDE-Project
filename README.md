# 🚌 MBTA Real-Time & Batch Transit Lakehouse Platform

[![CI: Build & Publish Container](https://github.com/dipk6545/CDE-Project/actions/workflows/build-image.yml/badge.svg)](https://github.com/dipk6545/CDE-Project/actions/workflows/build-image.yml)
[![CD: Sync Grafana Dashboard](https://github.com/dipk6545/CDE-Project/actions/workflows/sync-grafana.yml/badge.svg)](https://github.com/dipk6545/CDE-Project/actions/workflows/sync-grafana.yml)
[![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.3-orange.svg)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10.3-teal.svg)](https://airflow.apache.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

An enterprise-grade, end-to-end cloud data engineering platform that processes streaming and batch public transit telemetry from the **Massachusetts Bay Transportation Authority (MBTA)**.

---

## 🏛️ Architecture Overview

The system adheres to the **Medallion Lakehouse Architecture** pattern deployed across AWS, Kubernetes, and Serverless Cloud databases:

```
[MBTA API v3] ──> [producer.py] ──> [Kafka 3.8 Broker (Docker)] ──> [consumer.py]
                                                                          │
                                                                          ▼ (boto3)
┌─────────────────────────────────────────────────────────────────────────┴────────┐
│                          AMAZON S3 SYDNEY LAKEHOUSE                              │
│                                                                                  │
│  🟡 BRONZE: s3://.../bronze/date=YYYY-MM-DD/bronze_stream.jsonl (Raw Logs)      │
│  ⚪ SILVER: s3://.../silver/date=YYYY-MM-DD/*.snappy.parquet (Cleaned PySpark)  │
│  🟢 GOLD:   s3://.../gold/route_performance/ & congestion_hotspots/ (KPIs)       │
└────────────────────────────────────┬─────────────────────────────────────────────┘
                                     │
                                     ▼ (Aggregated Scorecards)
                    [Neon Serverless PostgreSQL 18]
                                     │
                                     ▼ (Live Queries)
                          [Grafana Cloud Dashboard]
```

* **Continuous Ingestion**: Apache Kafka (KRaft mode) buffers live vehicle telemetry without dropped events.
* **Storage Layer**: Amazon S3 (Sydney `ap-southeast-2`) stores all raw JSONL and Snappy Parquet partitions.
* **Distributed Compute**: Apache PySpark 3.5.3 handles schema validation, deduplication, and KPI aggregations.
* **Orchestration**: Apache Airflow 2.10 on Kubernetes (Kind + Helm) with `KubernetesExecutor`.
* **Serving Layer**: Neon Serverless PostgreSQL holds a **rolling 7-day window** of KPI scorecards.
* **Observability**: Grafana Cloud monitoring network speeds, fleet moving/stopped split, and transit delay bottlenecks via **CI/CD Dashboards-as-Code**.

---

## 📦 Tech Stack & Dependencies

All dependencies and build configurations are defined declaratively in **[`pyproject.toml`](pyproject.toml)** (PEP 621):

* **Language**: Python 3.11+
* **Streaming**: `kafka-python-ng`, `httpx`
* **Compute**: `pyspark==3.5.3`, `pyarrow`, `pandas`
* **Storage & Cloud**: `boto3`, `botocore`, `awscli`
* **Database & ORM**: `psycopg2-binary`, `sqlalchemy`
* **Orchestration**: `apache-airflow>=2.10.3`
* **Visualization**: Grafana Cloud, `python-pptx`

---

## 🚀 How to Start and Run the Project

You can run this project in two modes:

### Mode 1: Dual-Codespace Cloud Architecture (Recommended)

To avoid resource starvation, run the streaming ingestion and Kubernetes orchestration in two separate Codespaces communicating via Amazon S3:

#### Step 1: Clone & Configure `.env`
Ensure your `.env` file exists with AWS credentials and database connections:
```bash
cp .env.example .env
# Edit .env with your AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AIRFLOW DB connection
```

#### Step 2: In Codespace #1 (Streaming Node)
Launch Kafka in Docker, start the Producer & Consumer, and open the live stream monitor:
```bash
bash scripts/start_streaming.sh
```
* **To stop**: Press `Ctrl + C` (cleanly shuts down Producer, Consumer, and the Kafka container).

#### Step 3: In Codespace #2 (Orchestration & Compute Node)
Launch Kind Kubernetes, deploy Airflow via Helm, and open Port 8080:
```bash
bash scripts/start_airflow_k8s.sh
```
1. Open the forwarded Port **8080** in your browser.
2. Login with credentials:
   - **Username**: `admin`
   - **Password**: `admin`
3. Toggle `mbta_transit_lakehouse_pipeline` to **Active** and click **Play ▶️ (Trigger DAG)**.
4. Watch all 4 pipeline stages execute and turn green:
   - `check_bronze_data_available` 🟩
   - `run_pyspark_silver_transformation` 🟩
   - `run_pyspark_gold_aggregations` 🟩
   - `pipeline_complete` 🟩

---

### Mode 2: Local Linux Machine (High-Spec Host)

Install project dependencies in a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Run streaming and orchestration in two separate terminal tabs:
```bash
# Terminal 1: Real-Time Streaming Ingestion
bash scripts/start_streaming.sh

# Terminal 2: Kubernetes Airflow Orchestration
bash scripts/start_airflow_k8s.sh
```

---

## 📊 Live Observability & Dashboards

The executive transit dashboard is hosted on **Grafana Cloud**:
👉 `https://charmingjuniper1515.grafana.net/d/mbta-transit-lakehouse/mbta-transit-lakehouse-dashboard`

### What the 5 Dashboard Panels Show:
1. **Total GPS Pings Analyzed**: Overall volume and ingestion throughput of the fleet.
2. **Fleet Average Speed**: Single weighted velocity KPI tracking city-wide congestion (alert threshold: < 10 mph).
3. **Average Speed by Route**: Horizontal bar chart comparing transit corridor speeds without overlapping labels.
4. **Live Fleet Status (Moving vs. Stopped)**: Donut chart displaying the ratio of buses cruising (~60%) vs. stopped at stations/red lights (~40%).
5. **Top 5 Transit Bottlenecks & Hotspots**: Ranked table identifying specific bus stops with severe delays and high stuck-vehicle counts.

---

## 🔄 Automated CI/CD (Dashboards & Containers as Code)

* **`.github/workflows/build-image.yml`**: On push to `main` modifying code or Dockerfile, automatically builds and publishes the production image to GitHub Container Registry:
  `ghcr.io/dipk6545/cde-airflow:latest`
* **`.github/workflows/sync-grafana.yml`**: On push to `main` modifying `orchestration/grafana_transit_dashboard.json`, automatically updates the live Grafana Cloud dashboard via REST API in under 8 seconds.

---

## 📄 Complete Architectural Documentation

For the exhaustive A-to-Z reference covering schema definitions, storage retention rules, and technical interview explanations, refer to:
👉 **[`TRANSIT_LAKEHOUSE_COMPLETE_GUIDE.md`](TRANSIT_LAKEHOUSE_COMPLETE_GUIDE.md)**
