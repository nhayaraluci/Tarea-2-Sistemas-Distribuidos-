from kafka import KafkaConsumer, KafkaProducer
import requests
import json
import os
import time
import random

time.sleep(10)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
CACHE_URL = os.getenv("CACHE_URL", "http://cache-service:8000/query")

TOPIC_MAIN = "queries"
TOPIC_RETRY = "queries.retry"
TOPIC_DLQ = "queries.dlq"

MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_DELAY = float(os.getenv("RETRY_DELAY_SECONDS", 2))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"
)

consumer = KafkaConsumer(
    TOPIC_MAIN,
    TOPIC_RETRY,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="cache-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

metrics = {
    "processed": 0,
    "success": 0,
    "errors": 0,
    "retries": 0,
    "dlq": 0
}

# =========================
# SEND FUNCTIONS
# =========================

def send_retry(query):
    query = dict(query)

    query["_retry_count"] = query.get("_retry_count", 0) + 1
    query["_status"] = "RETRY"

    metrics["retries"] += 1

    print(f"🔁 RETRY -> {query['_retry_count']}")

    producer.send(TOPIC_RETRY, query)
    producer.flush()


def send_dlq(query):
    query = dict(query)

    query["_status"] = "DLQ"
    metrics["dlq"] += 1

    print("💀 SENT TO DLQ")

    producer.send(TOPIC_DLQ, query)
    producer.flush()


# =========================
# PROCESS
# =========================

def process(query):
    force = query.get("force_fail")

    if force == "DLQ":
        raise Exception("FORCED DLQ ERROR")

    if force == "RETRY":
        if random.random() < 0.7:
            raise Exception("FORCED RETRY ERROR")

    if random.random() < 0.1:
        raise Exception("Random failure")

    resp = requests.post(CACHE_URL, json=query, timeout=5)

    if resp.status_code >= 500:
        raise Exception("Cache error")

    return resp.json()


# =========================
# LOOP
# =========================

for msg in consumer:
    query = msg.value
    start = time.time()

    retry_c = query.get("_retry_count", 0)

    print("\n==============================")
    print("📩 MESSAGE:", query)

    try:
        process(query)

        metrics["success"] += 1
        print("✅ SUCCESS")

    except Exception as e:
        metrics["errors"] += 1
        print("❌ ERROR:", e)

        retry_c += 1
        query["_retry_count"] = retry_c

        if retry_c >= MAX_RETRIES:
            send_dlq(query)
        else:
            time.sleep(RETRY_DELAY)
            send_retry(query)

    finally:
        metrics["processed"] += 1