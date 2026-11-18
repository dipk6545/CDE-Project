import json
import os
from datetime import datetime, timezone
import httpx
from kafka import KafkaConsumer

KAFKA_TOPIC = "transit-vehicles"
BOOTSTRAP_SERVERS = ["localhost:9092"]
OUTPUT_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/bronze"))

def get_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="transit-bronze-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

def main():
    consumer = get_consumer()
    print(f"Consumer started. Listening to '{KAFKA_TOPIC}'...")

    try:
        for message in consumer:
            record = message.value
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            partition_dir = os.path.join(OUTPUT_BASE_DIR, f"date={today}")
            os.makedirs(partition_dir, exist_ok=True)

            file_path = os.path.join(partition_dir, "bronze_stream.jsonl")
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")

            print(f"Stored -> Vehicle: {record['vehicle_id']} | Route: {record['route_id']} | Speed: {record['speed']} mph")
    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
