import json
import os
import time
from datetime import datetime, timezone
import boto3
from kafka import KafkaConsumer

KAFKA_TOPIC = "transit-vehicles"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
OUTPUT_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/bronze"))
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "cde-transit-lakehouse-221950623036-ap-southeast-2")
S3_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-2")
CDE_ENV = os.environ.get("CDE_ENVIRONMENT", "local").lower()

def get_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="transit-bronze-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

def sync_bronze_file_to_s3(local_path: str, target_date: str):
    """Uploads the accumulated Bronze JSONL stream partition to Amazon S3."""
    if CDE_ENV != "cloud":
        return
    try:
        s3 = boto3.client("s3", region_name=S3_REGION)
        s3_key = f"bronze/date={target_date}/bronze_stream.jsonl"
        s3.upload_file(local_path, S3_BUCKET, s3_key)
        print(f"[Cloud Sync] Uploaded Bronze stream to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"[Cloud Sync Warning] S3 upload error: {e}")

def main():
    consumer = get_consumer()
    print(f"Consumer started in [{CDE_ENV.upper()} MODE]. Listening to topic '{KAFKA_TOPIC}'...")

    msg_count = 0
    last_sync_time = time.time()

    try:
        for message in consumer:
            record = message.value
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            partition_dir = os.path.join(OUTPUT_BASE_DIR, f"date={today}")
            os.makedirs(partition_dir, exist_ok=True)

            file_path = os.path.join(partition_dir, "bronze_stream.jsonl")
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")

            msg_count += 1
            print(f"Stored -> Vehicle: {record.get('vehicle_id')} | Route: {record.get('route_id')} | Speed: {record.get('speed')} mph")

            # In cloud mode, sync to S3 every 50 records or every 60 seconds
            if CDE_ENV == "cloud":
                current_time = time.time()
                if msg_count >= 50 or (current_time - last_sync_time) > 60:
                    sync_bronze_file_to_s3(file_path, today)
                    msg_count = 0
                    last_sync_time = current_time

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
    finally:
        # Final flush on shutdown if running in cloud
        if CDE_ENV == "cloud":
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            final_file = os.path.join(OUTPUT_BASE_DIR, f"date={today}", "bronze_stream.jsonl")
            if os.path.exists(final_file):
                sync_bronze_file_to_s3(final_file, today)
        consumer.close()

if __name__ == "__main__":
    main()
