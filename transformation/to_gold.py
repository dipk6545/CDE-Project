import os
from datetime import datetime, timezone
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round, sum as spark_sum, when

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")
S3_BUCKET = "cde-transit-lakehouse-221950623036-ap-southeast-2"
S3_REGION = "ap-southeast-2"

def ensure_silver_from_s3(target_date: str):
    """Downloads Silver Parquet from S3 if running in an isolated container."""
    local_dir = os.path.join(SILVER_DIR, f"date={target_date}")
    os.makedirs(local_dir, exist_ok=True)

    # If local silver folder has no parquet files, fetch from S3
    existing_parquet = [f for f in os.listdir(local_dir) if f.endswith(".parquet")]
    if not existing_parquet:
        s3 = boto3.client("s3", region_name=S3_REGION)
        prefix = f"silver/date={target_date}/"
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        files = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".parquet")]
        if not files:
            # Fallback to any latest silver date partition in S3
            resp_all = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="silver/")
            all_files = [obj["Key"] for obj in resp_all.get("Contents", []) if obj["Key"].endswith(".parquet")]
            if all_files:
                latest_date_part = all_files[-1].split("/")[1]
                print(f"[Cloud Mode] Target date not in Silver S3. Using: {latest_date_part}")
                prefix = f"silver/{latest_date_part}/"
                resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
                files = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".parquet")]

        for key in files:
            dest = os.path.join(local_dir, os.path.basename(key))
            print(f"[Cloud Mode] Syncing Silver from S3: s3://{S3_BUCKET}/{key} -> {dest}")
            s3.download_file(S3_BUCKET, key, dest)

def sync_gold_to_s3(local_dir: str, s3_prefix: str):
    """If CDE_ENVIRONMENT == 'cloud', syncs Gold KPIs directly to Amazon S3."""
    env_mode = os.environ.get("CDE_ENVIRONMENT", "local").lower()
    if env_mode != "cloud":
        print(f"[Local Mode] Gold KPI saved locally at {local_dir}. S3 upload skipped.")
        return

    s3 = boto3.client("s3", region_name=S3_REGION)
    for root, _, files in os.walk(local_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, local_dir)
            s3_key = f"{s3_prefix}/{rel_path}".replace("//", "/")
            s3.upload_file(full_path, S3_BUCKET, s3_key)
    print(f"[Cloud Mode] Gold KPI pushed to S3: s3://{S3_BUCKET}/{s3_prefix}")

def get_spark_session():
    return (
        SparkSession.builder
        .appName("MBTA-Silver-to-Gold")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

def run_gold_aggregation(target_date: str = None):
    if not target_date:
        target_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    ensure_silver_from_s3(target_date)

    input_path = os.path.join(SILVER_DIR, f"date={target_date}", "*.parquet")
    
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Starting PySpark Silver-to-Gold Job for date={target_date}...")

    # 1. Read Silver Parquet
    try:
        df_silver = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Error reading Silver path {input_path}: {e}")
        spark.stop()
        return

    row_count = df_silver.count()
    print(f"Loaded {row_count} records from Silver layer.")

    # 2. KPI 1: Route Performance (Average Speed & Stopped Vehicle Counts)
    df_route_metrics = (
        df_silver
        .groupBy("route_id")
        .agg(
            count("vehicle_id").alias("total_pings"),
            spark_round(avg("speed"), 2).alias("avg_speed_mph"),
            spark_sum(when(col("status") == "STOPPED_AT", 1).otherwise(0)).alias("stopped_count"),
            spark_sum(when(col("status") == "IN_TRANSIT_TO", 1).otherwise(0)).alias("in_transit_count")
        )
        .orderBy(col("total_pings").desc())
    )

    route_out = os.path.join(GOLD_DIR, "route_performance", f"date={target_date}")
    df_route_metrics.coalesce(1).write.mode("overwrite").parquet(route_out)
    sync_gold_to_s3(route_out, f"gold/route_performance/date={target_date}")
    print(f" - Gold KPI 1 (Route Performance) saved to: {route_out}")

    # 3. KPI 2: Congestion Hotspots (Stops with slowest moving vehicles)
    df_congestion = (
        df_silver
        .filter(col("stop_id") != "UNKNOWN")
        .groupBy("stop_id", "route_id")
        .agg(
            count("vehicle_id").alias("visit_count"),
            spark_round(avg("speed"), 2).alias("avg_speed_near_stop"),
            spark_sum(when(col("speed") == 0.0, 1).otherwise(0)).alias("zero_speed_pings")
        )
        .filter(col("visit_count") >= 2)
        .orderBy(col("avg_speed_near_stop").asc(), col("visit_count").desc())
    )

    congestion_out = os.path.join(GOLD_DIR, "congestion_hotspots", f"date={target_date}")
    df_congestion.coalesce(1).write.mode("overwrite").parquet(congestion_out)
    sync_gold_to_s3(congestion_out, f"gold/congestion_hotspots/date={target_date}")
    print(f" - Gold KPI 2 (Congestion Hotspots) saved to: {congestion_out}")

    print("\n=== Sample Route Performance Metrics ===")
    df_route_metrics.show(5, truncate=False)

    print("=== Sample Congestion Hotspots ===")
    df_congestion.show(5, truncate=False)

    spark.stop()

if __name__ == "__main__":
    run_gold_aggregation()
