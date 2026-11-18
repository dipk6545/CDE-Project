import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
try:
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
except ImportError:
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.providers.standard.operators.bash import BashOperator

# Path references
CDE_ROOT = "/mnt/HDD/MDEP-CDE Project/CDE"
CONDA_ENV = "/home/deep/anaconda3/envs/cde-py311"
ENV_PYTHON = os.path.join(CONDA_ENV, "bin/python")

default_args = {
    "owner": "cde_admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def check_bronze_data_sensor(**kwargs):
    """Verifies that bronze raw stream data exists before processing."""
    today = datetime.now().strftime("%Y-%m-%d")
    bronze_path = os.path.join(CDE_ROOT, f"data/bronze/date={today}/bronze_stream.jsonl")
    
    if not os.path.exists(bronze_path):
        raise FileNotFoundError(f"Bronze data missing for {today}: {bronze_path}")
    
    size_bytes = os.path.getsize(bronze_path)
    if size_bytes == 0:
        raise ValueError(f"Bronze file is empty: {bronze_path}")
        
    print(f"Verified: Bronze data exists ({size_bytes} bytes). Ready for transformation.")

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
        bash_command=f'export JAVA_HOME="{CONDA_ENV}" && export PATH="$JAVA_HOME/bin:$PATH" && "{ENV_PYTHON}" "{CDE_ROOT}/transformation/to_silver.py"',
    )

    # Task 3: PySpark Gold Aggregations (Route Metrics & Congestion Hotspots)
    task_gold = BashOperator(
        task_id="run_pyspark_gold_aggregations",
        bash_command=f'export JAVA_HOME="{CONDA_ENV}" && export PATH="$JAVA_HOME/bin:$PATH" && "{ENV_PYTHON}" "{CDE_ROOT}/transformation/to_gold.py"',
    )

    # Define DAG execution dependency chain: Sensor -> Silver -> Gold
    task_sensor >> task_silver >> task_gold
