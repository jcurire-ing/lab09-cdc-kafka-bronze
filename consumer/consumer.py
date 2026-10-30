from kafka import KafkaConsumer
import boto3
import json
import time
from datetime import datetime, timezone

TOPIC = "ucv.public.sales"
BUCKET = "ucv-data-lake"
BATCH_SIZE = 5

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id="ucv-bronze-writer",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    api_version=(2, 8, 0),  # Fix para el selector de sockets en Windows
    value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b else None,
)

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

try:
    s3.head_bucket(Bucket=BUCKET)
except Exception:
    s3.create_bucket(Bucket=BUCKET)

batch = []

def flush(records):
    if not records:
        return
    now = datetime.now(timezone.utc)
    key = (
        f"bronze/sales/year={now:%Y}/month={now:%m}/day={now:%d}/"
        f"sales_cdc_{now:%Y%m%dT%H%M%S}_{int(time.time()*1000)}.jsonl"
    )
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    print(f"[S3] s3://{BUCKET}/{key} ({len(records)} eventos)")

print(f"Escuchando topic {TOPIC}...")

try:
    for msg in consumer:
        event = {
            "kafka_topic": msg.topic,
            "kafka_partition": msg.partition,
            "kafka_offset": msg.offset,
            "kafka_timestamp": msg.timestamp,
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": msg.value,
        }
        batch.append(event)
        print(f"offset={msg.offset} op={(msg.value or {}).get('op')}")
        if len(batch) >= BATCH_SIZE:
            flush(batch)
            batch.clear()
except KeyboardInterrupt:
    print("Deteniendo consumer...")
finally:
    flush(batch)
    consumer.close()