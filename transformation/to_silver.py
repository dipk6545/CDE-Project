import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")

def get_spark_session():
    return (
        SparkSession.builder
        .appName("MBTA-Bronze-to-Silver")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

def run_silver_transformation(target_date: str = None):
    if not target_date:
        target_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    input_path = os.path.join(BRONZE_DIR, f"date={target_date}", "*.jsonl")
    out_dir = os.path.join(SILVER_DIR, f"date={target_date}")

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Starting PySpark Bronze-to-Silver Job for date={target_date}...")

    # 1. Read JSONL directly with Spark DataFrame reader
    try:
        df_raw = spark.read.json(input_path)
    except Exception as e:
        print(f"Error reading path {input_path}: {e}")
        spark.stop()
        return

    initial_count = df_raw.count()
    print(f"Raw records ingested: {initial_count}")

    # 2. Deduplication using PySpark dropDuplicates
    df_dedup = df_raw.dropDuplicates(["vehicle_id", "timestamp_utc"])

    # 3. Clean and enforce schema types using PySpark DataFrame API
    df_clean = (
        df_dedup
        .withColumn("speed", when(col("speed").isNull(), 0.0).otherwise(col("speed").cast("double")))
        .withColumn("bearing", when(col("bearing").isNull(), 0.0).otherwise(col("bearing").cast("double")))
        .withColumn("current_stop_sequence", when(col("current_stop_sequence").isNull(), 0).otherwise(col("current_stop_sequence").cast("integer")))
        .withColumn("hour_of_day", col("hour_of_day").cast("integer"))
        .withColumn("status", when(col("status").isNull(), "UNKNOWN").otherwise(col("status").cast("string")))
        .withColumn("route_id", when(col("route_id").isNull(), "UNKNOWN").otherwise(col("route_id").cast("string")))
        .select(
            "vehicle_id", "route_id", "trip_id", "stop_id",
            "status", "latitude", "longitude", "speed",
            "bearing", "current_stop_sequence", "timestamp_utc", "hour_of_day"
        )
    )

    clean_count = df_clean.count()

    # 4. Write partitioned Snappy Parquet using Spark writer
    (
        df_clean
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(out_dir)
    )

    print("PySpark Silver Transformation Complete:")
    print(f" - Input rows : {initial_count}")
    print(f" - Output rows: {clean_count} (deduplicated)")
    print(f" - Parquet written to: {out_dir}")

    spark.stop()

if __name__ == "__main__":
    run_silver_transformation()
