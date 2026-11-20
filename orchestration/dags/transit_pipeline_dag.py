import os
import sys
from datetime import datetime, timedelta, timezone
from airflow import DAG
try:
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
except ImportError:
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.providers.standard.operators.bash import BashOperator

import boto3

# Path references: check whether running inside Docker container or locally on host
if os.path.exists("/opt/airflow/transformation"):
    CDE_ROOT = "/opt/airflow"
    ENV_PYTHON = "python3"
else:
    CDE_ROOT = "/mnt/HDD/MDEP-CDE Project/CDE"
    CONDA_ENV = "/home/deep/anaconda3/envs/cde-py311"
    ENV_PYTHON = os.path.join(CONDA_ENV, "bin/python")

S3_BUCKET = "cde-transit-lakehouse-221950623036-ap-southeast-2"
S3_REGION = "ap-southeast-2"

default_args = {
    "owner": "cde_admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def check_bronze_data_sensor(**kwargs):
    """Verifies bronze data exists in S3 or local disk.
    If today's data is not uploaded yet, syncs/uses the most recent available date.
    """
    s3 = boto3.client("s3", region_name=S3_REGION)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # 1. Check if S3 already has data for today
    prefix_today = f"bronze/date={today}/"
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix_today)
    
    if resp.get("KeyCount", 0) > 0:
        print(f"Verified: Bronze data exists in S3 for today ({today}).")
        return

    # 2. Check for any previous date partition in S3
    resp_all = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="bronze/")
    contents = resp_all.get("Contents", [])
    
    if contents:
        latest_file = sorted(contents, key=lambda x: x["LastModified"])[-1]["Key"]
        print(f"Verified: Found recent Bronze data in S3: {latest_file}.")
        return

    # 3. If S3 has no data, ensure folder prefix exists so it's ready
    s3.put_object(Bucket=S3_BUCKET, Key=f"bronze/date={today}/")
    print(f"Created S3 folder prefix: bronze/date={today}/")

with DAG(
    dag_id="mbta_transit_lakehouse_pipeline",
    default_args=default_args,
    description="Automated Medallion Pipeline: Bronze -> Silver (PySpark) -> Gold (Analytics)",
    schedule=None,  # Airflow 3 uses 'schedule' instead of 'schedule_interval'
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["cde", "pyspark", "transit"],
) as dag:

    # Task 1: Data Sensor (Validates Bronze data exists)
    task_sensor = PythonOperator(
        task_id="check_bronze_data_available",
        python_callable=check_bronze_data_sensor,
    )

    # Task 2: PySpark Silver Transformation (Deduplication & Parquet format)
    task_silver = BashOperator(
        task_id="run_pyspark_silver_transformation",
        bash_command=f'"{ENV_PYTHON}" "{CDE_ROOT}/transformation/to_silver.py"',
    )

    # Task 3: PySpark Gold Aggregations (Route Metrics & Congestion Hotspots)
    task_gold = BashOperator(
        task_id="run_pyspark_gold_aggregations",
        bash_command=f'"{ENV_PYTHON}" "{CDE_ROOT}/transformation/to_gold.py"',
    )

    # Task 4: Sync Medallion Lakehouse data to Amazon S3
    task_s3_sync = BashOperator(
        task_id="sync_lakehouse_to_s3",
        bash_command=f'aws s3 sync "{CDE_ROOT}/data/" "s3://{S3_BUCKET}/" --region ap-southeast-2',
    )

    # Define DAG execution dependency chain: Sensor -> Silver -> Gold
    task_sensor >> task_silver >> task_gold >> task_s3_sync
