from kafka import KafkaConsumer, KafkaProducer
import json
import os
import time
import requests

time.sleep(10)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
CACHE_URL = os.getenv("CACHE_URL", "http://cache-service:8000/query")

TOPIC_RETRY = "queries.retry"
TOPIC_DLQ = "queries.dlq"

MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"
)

consumer = KafkaConsumer(
    TOPIC_RETRY,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="retry-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("🔁 RETRY WORKER STARTED")


def send_dlq(query):
    query = dict(query)
    query["_status"] = "DLQ"

    producer.send(TOPIC_DLQ, query)
    producer.flush()

    print("💀 SENT TO DLQ")


def process(query):
    force = query.get("force_fail")

    # Fuerza envío a DLQ
    if force == "DLQ":
        raise Exception("FORCED DLQ ERROR")

    # Fuerza reintentos hasta llegar a DLQ
    if force == "RETRY":
        raise Exception("FORCED RETRY ERROR")

    # Procesamiento normal
    resp = requests.post(CACHE_URL, json=query, timeout=5)

    if resp.status_code >= 500:
        raise Exception("Cache error")

    return resp.json()


for msg in consumer:
    query = msg.value

    retry_count = int(query.get("_retry_count", 0))

    print("\n==================================================")
    print("🔁 RETRY MESSAGE")
    print(query)
    print(f"retry_count = {retry_count}")
    print("==================================================")

    try:
        process(query)

        print("✅ RETRY SUCCESS")

    except Exception as e:
        print(f"❌ RETRY ERROR: {e}")

        retry_count += 1
        query["_retry_count"] = retry_count

        if retry_count >= MAX_RETRIES:
            print(f"💀 MAX RETRIES REACHED ({retry_count})")
            send_dlq(query)

        else:
            print(f"🔁 RE-RETRY -> {retry_count}")

            producer.send(TOPIC_RETRY, query)
            producer.flush()