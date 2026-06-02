from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import os
import time

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

consumer = None

while consumer is None:
    try:
        print("⏳ Connecting to Kafka...")

        consumer = KafkaConsumer(
            "queries.dlq",
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="dlq-worker",
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )

        print("💀 DLQ WORKER STARTED")

    except NoBrokersAvailable:
        print("⚠️ Kafka not ready. Retrying in 5 seconds...")
        time.sleep(5)

for msg in consumer:
    query = msg.value

    print("\n===================================")
    print("💀 DLQ MESSAGE RECEIVED")
    print("===================================")

    retry_count = int(query.get("_retry_count", 0))

    print(f"query_id    : {query.get('query_id')}")
    print(f"query_type  : {query.get('query_type')}")
    print(f"retry_count : {retry_count}")
    print(f"force_fail  : {query.get('force_fail')}")

    if retry_count < 3:
        print("⚠️ INVALID DLQ MESSAGE (retry_count < 3)")
        continue

    print("💀 FINAL FAILED MESSAGE")
    print(json.dumps(query, indent=2))