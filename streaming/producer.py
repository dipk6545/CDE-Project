import json
import time
from datetime import datetime, timezone
import httpx
from kafka import KafkaProducer

API_URL = "https://api-v3.mbta.com/vehicles?include=trip,stop&page[limit]=20"
KAFKA_TOPIC = "transit-vehicles"
BOOTSTRAP_SERVERS = ["localhost:9092"]

def get_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def clean_record(v):
    attr = v.get("attributes", {})
    rel = v.get("relationships", {})

    lat = attr.get("latitude")
    lon = attr.get("longitude")
    if lat is None or lon is None:
        return None

    speed = float(attr.get("speed")) if attr.get("speed") is not None else 0.0
    bearing = float(attr.get("bearing")) if attr.get("bearing") is not None else 0.0

    def extract_id(rel_dict, key):
        data = rel_dict.get(key, {}).get("data")
        return data.get("id") if isinstance(data, dict) else "UNKNOWN"

    updated_at = attr.get("updated_at")
    dt = datetime.fromisoformat(updated_at).astimezone(timezone.utc) if updated_at else datetime.now(timezone.utc)

    return {
        "vehicle_id": v.get("id"),
        "route_id": extract_id(rel, "route"),
        "trip_id": extract_id(rel, "trip"),
        "stop_id": extract_id(rel, "stop"),
        "status": attr.get("current_status", "UNKNOWN"),
        "latitude": round(float(lat), 6),
        "longitude": round(float(lon), 6),
        "speed": round(speed, 2),
        "bearing": round(bearing, 1),
        "current_stop_sequence": int(attr.get("current_stop_sequence") or 0),
        "timestamp_utc": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "hour_of_day": dt.hour
    }

def main():
    producer = get_producer()
    print(f"Producer started. Streaming to topic '{KAFKA_TOPIC}'...")

    client = httpx.Client(headers={"User-Agent": "CDE-Producer/1.0"}, timeout=10.0)

    try:
        while True:
            resp = client.get(API_URL)
            if resp.status_code == 200:
                vehicles = resp.json().get("data", [])
                sent_count = 0
                for v in vehicles:
                    record = clean_record(v)
                    if record:
                        producer.send(KAFKA_TOPIC, value=record)
                        sent_count += 1
                producer.flush()
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Published {sent_count} records to Kafka.")
            else:
                print(f"API Error: HTTP {resp.status_code}")

            time.sleep(5)
    except KeyboardInterrupt:
        print("\nProducer stopped.")
    finally:
        client.close()
        producer.close()

if __name__ == "__main__":
    main()
