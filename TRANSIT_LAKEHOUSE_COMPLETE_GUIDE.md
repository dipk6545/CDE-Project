# 🚌 MBTA Real-Time & Batch Transit Lakehouse: End-to-End Architecture Guide (A to Z)

---

## 📌 Executive Summary
The **MBTA Transit Lakehouse** is an enterprise-grade cloud data engineering platform that processes live public transit telemetry from the Massachusetts Bay Transportation Authority (MBTA). It ingests streaming GPS vehicle coordinates, stores and organizes them via a **Medallion Lakehouse Architecture** on **Amazon S3 Sydney**, performs distributed batch aggregations using **Apache PySpark**, orchestrates workflows via **Apache Airflow on Kubernetes (Kind + Helm)** with the `KubernetesExecutor`, serves KPIs via **Prisma Cloud PostgreSQL**, and delivers real-time analytics to **Grafana Cloud** with automated **CI/CD Dashboards-as-Code**.

---

## 🏗️ 1. Complete System Architecture & Data Flow

```
                                      [ REAL-TIME INGESTION LAYER ]
       +------------------------------------+
       |  MBTA Live REST API v3             |
       |  (api-v3.mbta.com/vehicles)        |
       +-----------------+------------------+
                         |  (HTTPS Poll / JSON)
                         v
       +-----------------+------------------+
       |  streaming/producer.py             |
       |  (Kafka Producer daemon)           |
       +-----------------+------------------+
                         |  (JSON payloads)
                         v
       +-----------------+------------------+
       |  Apache Kafka 3.8 / 4.4 Broker     |
       |  (KRaft Mode, Topic: transit-veh)  |
       +-----------------+------------------+
                         |  (Consumer Group: cde-bronze-group)
                         v
       +-----------------+------------------+
       |  streaming/consumer.py             |
       |  (Micro-batch buffer: 50 rec / 60s)|
       +-----------------+------------------+
                         |
                         v (boto3 upload)
+-------------------------------------------------------------------------------+
|                      2. MEDALLION LAKEHOUSE STORAGE (AWS S3)                  |
|                                                                               |
|  🟡 BRONZE LAYER (Raw Telemetry)                                              |
|     s3://cde-transit-lakehouse.../bronze/date=YYYY-MM-DD/bronze_stream.jsonl  |
|     - Unaltered, raw audit logs for disaster recovery and schema replay       |
|                                                                               |
|  ⚪ SILVER LAYER (Cleaned Columnar Parquet)                                    |
|     s3://cde-transit-lakehouse.../silver/date=YYYY-MM-DD/*.snappy.parquet     |
|     - Deduplicated on (vehicle_id, timestamp_utc), validated types            |
|                                                                               |
|  🟢 GOLD LAYER (Aggregated Business Analytics)                                |
|     s3://cde-transit-lakehouse.../gold/route_performance/date=YYYY-MM-DD/     |
|     s3://cde-transit-lakehouse.../gold/congestion_hotspots/date=YYYY-MM-DD/   |
+-------------------------------------------------------------------------------+
                         ^                     ^
                         | (Read Bronze)       | (Read Silver & Write Gold)
                         |                     |
+------------------------+---------------------+--------------------------------+
|                 3. DISTRIBUTED COMPUTE LAYER (Apache PySpark)                 |
|                                                                               |
|  • transformation/to_silver.py                                                |
|    Enforces typed schema, casts speeds/coords, deduplicates pings             |
|                                                                               |
|  • transformation/to_gold.py                                                  |
|    KPI 1: Route velocities, total pings, moving vs stopped count              |
|    KPI 2: Stop-level bottlenecks and idle vehicle hotspots                    |
+-------------------------------------------------------------------------------+
                         ^
                         | (Scheduled / Executed via Pods)
+------------------------+------------------------------------------------------+
|             4. ORCHESTRATION & CLOUD INFRASTRUCTURE (Airflow on K8s)          |
|                                                                               |
|  • Apache Airflow 2.10.3 / 2.10.5 on Kubernetes (Kind Cluster 'cde-cluster')  |
|  • KubernetesExecutor: On-demand isolated worker pods per task                |
|  • Docker Image: ghcr.io/dipk6545/cde-airflow:latest                          |
|    (Ubuntu, Java 17, PySpark 3.5.3, AWS CLI, PostgreSQL psycopg2)             |
|  • DAG: orchestration/dags/transit_pipeline_dag.py                            |
|    [Task 1: S3 Sensor] -> [Task 2: Silver] -> [Task 3: Gold] -> [Task 4: End]|
+------------------------+------------------------------------------------------+
                         |
                         | (Writes Aggregated Scorecards)
                         v
+-------------------------------------------------------------------------------+
|                      5. SERVING DATABASE (Prisma Cloud PostgreSQL)            |
|                                                                               |
|  • Host: db.prisma.io:5432 / postgres (PostgreSQL 17.2)                       |
|  • Stores Airflow Metadata (50 tables)                                        |
|  • Serving Tables:                                                            |
|    - kpi_route_performance (route_id, avg_speed_mph, total_pings, updated_at) |
|    - kpi_congestion_hotspots (stop_id, route_id, avg_speed, zero_speed_pings) |
+-------------------------------------------------------------------------------+
                         ^
                         | (Dynamic SQL with $__timeFilter)
+------------------------+------------------------------------------------------+
|                     6. OBSERVABILITY & DASHBOARDS-AS-CODE                     |
|                                                                               |
|  • Grafana Cloud (charmingjuniper1515.grafana.net)                            |
|  • Dashboard: MBTA Transit Lakehouse Dashboard (5 Real-Time Panels)           |
|  • CI/CD Automation: .github/workflows/sync-grafana.yml                       |
|    Git Push to main -> GitHub Actions -> Grafana REST API (< 8s live sync)   |
+-------------------------------------------------------------------------------+
```

---

## 🛠️ 2. Comprehensive Technology Stack (A to Z)

| Technology | Category | Purpose in Project | Key Configuration |
| :--- | :--- | :--- | :--- |
| **Amazon S3** | Cloud Object Storage | Medallion Lakehouse backing store for Bronze, Silver, and Gold partitions. | Bucket: `cde-transit-lakehouse-221950623036-ap-southeast-2` (Sydney) |
| **Apache Airflow** | Workflow Orchestrator | Schedules, monitors, and executes multi-stage data processing pipeline. | v2.10.3 / 2.10.5 with `KubernetesExecutor` on daily cadence (`@daily`) |
| **Apache Kafka** | Event Streaming Broker | High-throughput buffer for real-time bus telemetry without dropping events. | KRaft mode (no ZooKeeper), running in Docker (`apache/kafka:3.8.0`) on `9092` |
| **Apache PySpark** | Distributed Compute | Large-scale schema enforcement, deduplication, and KPI aggregations. | PySpark 3.5.3 running on Java 17 with Snappy Parquet compression |
| **Boto3** | AWS SDK for Python | Programmatic upload and sync of Bronze JSONL and Gold Parquet to S3. | Integrated in `consumer.py`, `to_silver.py`, `to_gold.py`, and DAG sensor |
| **Docker & Docker Compose** | Containerization | Standardizes Kafka broker runtime and Airflow production images. | `docker-compose-kafka.yml`, `orchestration/Dockerfile` |
| **GitHub Actions** | CI/CD Automation | Continuous container image builds and automated Dashboards-as-Code syncing. | `.github/workflows/build-image.yml`, `.github/workflows/sync-grafana.yml` |
| **GitHub Container Registry (GHCR)** | Container Image Shelf | Stores and serves versioned production Airflow images. | `ghcr.io/dipk6545/cde-airflow:latest` |
| **Grafana Cloud** | BI & Observability | Real-time transit executive command dashboard with dynamic time filters. | `https://charmingjuniper1515.grafana.net` (Datasource: Prisma PostgreSQL) |
| **Helm** | Kubernetes Package Manager | Declarative deployment of production Apache Airflow Helm Chart. | Chart: `apache-airflow/airflow` (v1.16.0), values: `k8s-values.yaml` |
| **HTTPX** | Async/Sync HTTP Client | Fetches real-time vehicle GPS, trip, and stop metadata from MBTA REST API. | In `streaming/producer.py` with 10s timeouts and keep-alive connections |
| **Java (OpenJDK 17)** | JVM Runtime | Powers Apache PySpark worker nodes and Apache Kafka internal execution. | Pre-installed in `ghcr.io/dipk6545/cde-airflow:latest` (`JAVA_HOME`) |
| **Kind (Kubernetes-in-Docker)** | Local Kubernetes Cluster | Creates multi-node local Kubernetes clusters inside Docker containers. | Cluster: `cde-cluster`, context: `kind-cde-cluster` |
| **KubernetesExecutor** | Airflow Scheduler Engine | Launches an isolated, temporary Kubernetes worker pod per DAG task. | Auto-cleans pods on task completion, eliminating idle resource waste |
| **MBTA V3 API** | External Transit Source | Live vehicle telemetry source (latitude, longitude, speed, status, route). | `https://api-v3.mbta.com/vehicles?include=trip,stop` |
| **PostgreSQL (Prisma Cloud)** | Serverless Relational DB | Operational metadata store for Airflow + fast serving layer for Grafana. | PostgreSQL 17.2 at `db.prisma.io:5432/postgres?sslmode=require` |
| **Python 3.11** | Core Programming Language | Powers producers, consumers, PySpark transforms, Airflow DAGs, and monitors. | Standardized runtime across local environments and container images |

---

## 📂 3. Codebase Directory & File Catalog

```
CDE/
├── .github/
│   └── workflows/
│       ├── build-image.yml                  # CI: Rebuilds and pushes Docker image to GHCR on code push
│       └── sync-grafana.yml                 # CD: Deploys grafana_transit_dashboard.json via REST API
├── config/                                  # Environment and cluster configuration
├── data/                                    # Local lakehouse data directories (gitignored)
│   ├── bronze/                              # date=YYYY-MM-DD/bronze_stream.jsonl
│   ├── silver/                              # date=YYYY-MM-DD/*.snappy.parquet
│   └── gold/                                # route_performance/ & congestion_hotspots/
├── kafka/                                   # Kafka setup scripts and broker configurations
│   ├── config/server.properties             # Standalone KRaft properties
│   └── setup_kraft.sh                       # KRaft storage formatter
├── orchestration/
│   ├── dags/
│   │   └── transit_pipeline_dag.py          # 4-stage master Airflow DAG
│   ├── Dockerfile                           # Custom Airflow + Java 17 + PySpark 3.5.3 image definition
│   ├── grafana_transit_dashboard.json       # Version-controlled Grafana dashboard definition
│   └── k8s-values.yaml                      # Production Helm values for KubernetesExecutor & Prisma DB
├── scripts/
│   ├── monitor_stream.py                    # Live terminal visualizer (speeds, vehicles, S3 uploads)
│   ├── start_all.sh                         # Master all-in-one startup automation
│   ├── start_streaming.sh                   # Dedicated script for Docker Kafka + Producer + Consumer
│   ├── start_airflow_k8s.sh                 # Dedicated script for Kind + Helm + Airflow on Port 8080
│   └── start_codespaces.sh                  # Codespace bootstrap & Kubernetes secret injector
├── streaming/
│   ├── producer.py                          # MBTA API -> Kafka Producer
│   └── consumer.py                          # Kafka Consumer -> local buffer -> AWS S3 Bronze sink
├── transformation/
│   ├── to_silver.py                         # PySpark Bronze JSONL -> Silver Parquet
│   └── to_gold.py                           # PySpark Silver Parquet -> Gold Parquets + PostgreSQL sync
├── docker-compose-airflow.yml               # Standalone Docker Airflow runner
├── docker-compose-kafka.yml                 # Standalone Docker Kafka 3.8 (KRaft) runner
├── requirements.txt                         # Python dependencies
└── Transit_Lakehouse_Presentation.pptx      # 8-slide executive presentation deck
```

---

## 🔬 4. Detailed Component Breakdown

### 4.1 Ingestion & Streaming Layer
* **`streaming/producer.py`**:
  - Connects to MBTA API endpoint `https://api-v3.mbta.com/vehicles?include=trip,stop`.
  - Normalizes JSON payloads: extracts `vehicle_id`, `route_id`, `trip_id`, `stop_id`, `latitude`, `longitude`, `speed` (mph), and `bearing`.
  - Serializes dictionaries to UTF-8 JSON bytes and streams to Kafka topic `transit-vehicles`.
* **`docker-compose-kafka.yml`**:
  - Launches official `apache/kafka:3.8.0` in modern **KRaft mode** (no ZooKeeper requirement).
  - Exposes port `9092` with `PLAINTEXT://0.0.0.0:9092` and advertised listener `localhost:9092`.
* **`streaming/consumer.py`**:
  - Subscribes to `transit-vehicles` under consumer group `transit-bronze-group`.
  - Micro-batches records into local JSONL files partitioned by `date=YYYY-MM-DD`.
  - In `cloud` mode, it uses `boto3.client('s3')` to sync the micro-batch to AWS S3 Sydney every 50 records or 60 seconds.

### 4.2 Medallion Lakehouse Transformations
* **Bronze Layer (`s3://.../bronze/date=YYYY-MM-DD/bronze_stream.jsonl`)**:
  - Raw, unedited append-only record stream. Serves as immutable audit source.
* **`transformation/to_silver.py`**:
  - Reads raw JSONL from S3 using PySpark `spark.read.json()`.
  - Enforces schema: casts speeds to `DoubleType`, stop sequences to `IntegerType`, fills missing values with defaults.
  - Deduplicates events using `dropDuplicates(["vehicle_id", "timestamp_utc"])`.
  - Writes Snappy-compressed columnar Parquet partitioned by date to `s3://.../silver/date=YYYY-MM-DD/`.
* **`transformation/to_gold.py`**:
  - Reads Silver Parquet dataset using PySpark.
  - **KPI 1 (Route Performance)**: Computes `avg_speed_mph`, `total_pings`, `stopped_count`, and `in_transit_count` grouped by `route_id`.
  - **KPI 2 (Congestion Hotspots)**: Filters stops with $\ge 2$ visits and computes `avg_speed_near_stop` and `zero_speed_pings` (idle vehicles stuck in traffic).
  - Saves partitioned Gold Parquet tables in S3 and writes aggregated summary rows directly into Prisma PostgreSQL serving tables.

### 4.3 Orchestration & Kubernetes Architecture
* **`orchestration/dags/transit_pipeline_dag.py`**:
  - Configured with daily schedule (`@daily`), retries: 1, retry_delay: 1 min.
  - **Task 1: `check_bronze_data_available` (PythonOperator)**: S3 sensor verifying incoming `bronze_stream.jsonl`. Implements graceful fallback to recent partitions if streaming was paused.
  - **Task 2: `run_pyspark_silver_transformation` (BashOperator)**: Triggers `to_silver.py` inside a dedicated Kubernetes worker pod.
  - **Task 3: `run_pyspark_gold_aggregations` (BashOperator)**: Triggers `to_gold.py`, updating Gold S3 partitions and PostgreSQL serving tables.
  - **Task 4: `pipeline_complete` (PythonOperator)**: Execution barrier confirming pipeline SLA attainment.
* **`orchestration/Dockerfile`**:
  - Multi-stage production container based on `apache/airflow:2.10.3-python3.11`.
  - Installs OpenJDK 17 (`JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`).
  - Bundles PySpark 3.5.3, AWS CLI, Boto3, and PostgreSQL drivers directly inside the image, allowing standalone execution without host file mounting.

### 4.4 Serving Database & Grafana Observability
* **Prisma Cloud PostgreSQL 17.2**:
  - Serves as the operational metadata store for Airflow's 50 internal tables.
  - **7-Day Retention Window & Storage Optimization**:
    - PostgreSQL acts strictly as a **lightweight operational serving layer** holding only the **last 7 days of rolling aggregated KPI scorecards** (~5 to 10 summary rows per pipeline run, under 1 MB of disk space total).
    - It does **not** store historical raw logs or bulky row-level vehicle data.
    - Full historical data spanning months and years is permanently retained in **Amazon S3** in columnar Parquet format, preventing PostgreSQL storage exhaustion and maintaining sub-second query performance.
  - Serves as the high-speed serving layer for business analytics:
    ```sql
    CREATE TABLE IF NOT EXISTS kpi_route_performance (
        id SERIAL PRIMARY KEY,
        route_id VARCHAR(100),
        avg_speed_mph FLOAT,
        total_pings INT,
        stopped_count INT,
        in_transit_count INT,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    ```
* **Grafana Cloud Dashboard (`orchestration/grafana_transit_dashboard.json`)**:
  - **Panel 1: Total GPS Pings Analyzed**: Metric stat card showing total processed vehicle pings.
  - **Panel 2: Fleet Average Speed**: Gauge/stat tracking network velocity; alerts if speed drops below 10 mph.
  - **Panel 3: Average Vehicle Speed by Route**: Horizontal bar chart comparing transit corridors without label overlap.
  - **Panel 4: Live Fleet Status (Moving vs Stopped)**: Donut chart displaying proportion of vehicles cruising vs stopped at stations/signals.
  - **Panel 5: Top 5 Transit Bottlenecks & Hotspots**: Table detailing slow bus stops and stuck vehicle counts.
  - **Dynamic Time Filters**: Uses `$__timeFilter(updated_at)` and `COALESCE(..., 0)` so changing dropdowns (Last 5m, 1h, 24h, 7d) dynamically queries PostgreSQL.

---

## ⚡ 5. GitHub Actions CI/CD Pipelines

### 5.1 Automated Container Image Rebuild (`build-image.yml`)
* **Trigger**: Push to `main` modifying `orchestration/**`, `transformation/**`, `streaming/**`, or `docker-compose-kafka.yml`.
* **Action**: Logs into GitHub Container Registry (`ghcr.io`), builds `orchestration/Dockerfile`, and publishes `ghcr.io/dipk6545/cde-airflow:latest`.

### 5.2 Dashboards-as-Code Sync (`sync-grafana.yml`)
* **Trigger**: Push to `main` modifying `orchestration/grafana_transit_dashboard.json`.
* **Action**: Reads `GRAFANA_API_KEY` from repository secrets, calls Grafana Cloud REST API (`POST /api/dashboards/db`), and live-updates the production dashboard in under 8 seconds.

---

## 🚀 6. Execution Modes (Local vs Dual-Codespace Cloud Architecture)

This project supports two operational execution environments:

### Option A: Dual-Codespace Cloud Architecture (Recommended for GitHub Codespaces)
To replicate a true decoupled enterprise cloud deployment and avoid memory pressure on smaller virtual machines, the workload is partitioned across **two dedicated Codespaces communicating via Amazon S3**:

1. **Codespace #1: Streaming & Ingestion Node (Kafka Broker + Streamers)**
   - Dedicated exclusively to continuous real-time data ingestion.
   - Command:
     ```bash
     bash scripts/start_streaming.sh
     ```
   - **What it runs**: Starts Kafka 3.8 in Docker, launches `producer.py` and `consumer.py`, opens the live terminal visualizer, and streams micro-batches directly to **AWS S3 Sydney** (`bronze/`).
   - Press `Ctrl + C` at any time to cleanly stop Producer, Consumer, and Kafka.

2. **Codespace #2: Orchestration & Compute Cluster (Kind + Airflow + PySpark)**
   - Launched as a second independent Codespace (4-core / 16GB RAM recommended).
   - Command:
     ```bash
     bash scripts/start_airflow_k8s.sh
     ```
   - **What it runs**: Provisions a local Kind Kubernetes cluster (`cde-cluster`), deploys the official Apache Airflow Helm Chart with the `KubernetesExecutor`, connects to Prisma Cloud PostgreSQL, and forwards Port 8080.
   - **Airflow Web UI**: Open forwarded Port 8080 (Login: `admin` / `admin`) and trigger `mbta_transit_lakehouse_pipeline`.
   - **Data Flow**: Task 1 reads the fresh Bronze stream uploaded by Codespace #1 from AWS S3, PySpark cleans it into Silver Parquet, computes Gold aggregates, and updates PostgreSQL!

---

### Option B: Unified Single-Machine Execution (Local Linux / High-Spec Host)
On a local development machine with sufficient RAM and physical cores:
```bash
# Terminal 1: Real-Time Streaming Ingestion
bash scripts/start_streaming.sh

# Terminal 2: Kubernetes Airflow Orchestration
bash scripts/start_airflow_k8s.sh
```

---

## 🏆 7. Key Data Engineering Highlights for Interviews
1. **True Lakehouse Decoupling**: Heavy continuous streaming (Kafka) is decoupled from scheduled distributed batch processing (Airflow + PySpark).
2. **Cost-Efficient Serving Architecture**: Cheap AWS S3 Sydney object storage stores multi-gigabyte raw and columnar data; PostgreSQL stores only lightweight aggregated scorecard rows (< 1 MB/year), preventing database bloat.
3. **Resilient Kubernetes Architecture**: `KubernetesExecutor` runs isolated pods per task; out-of-memory errors in compute tasks do not impact the Airflow scheduler or webserver.
4. **Production CI/CD (Dashboards-as-Code)**: Zero manual JSON importing. Changes made in git are deployed automatically to Grafana Cloud via REST API.
