import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round, sum as spark_sum, when

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

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
    print(f" - Gold KPI 2 (Congestion Hotspots) saved to: {congestion_out}")

    print("\n=== Sample Route Performance Metrics ===")
    df_route_metrics.show(5, truncate=False)

    print("=== Sample Congestion Hotspots ===")
    df_congestion.show(5, truncate=False)

    spark.stop()

if __name__ == "__main__":
    run_gold_aggregation()
