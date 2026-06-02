from kafka import KafkaConsumer, KafkaProducer
import requests
import json
import os
import time
import random

time.sleep(10)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
CACHE_URL = os.getenv("CACHE_URL", "http://cache-service:8000/query")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"
)

consumer = KafkaConsumer(
    "queries",
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="main-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("🚀 MAIN CONSUMER STARTED")


def process(query):

    # Fallo forzado para pruebas DLQ
    if query.get("force_fail") == "DLQ":
        raise Exception("FORCED DLQ ERROR")

    # Fallo forzado para pruebas Retry
    if query.get("force_fail") == "RETRY":
        raise Exception("FORCED RETRY ERROR")

    # Fallos aleatorios normales
    if random.random() < 0.1:
        raise Exception("Random failure")

    resp = requests.post(
        CACHE_URL,
        json=query,
        timeout=5
    )

    if resp.status_code >= 500:
        raise Exception("Cache error")

    return resp.json()


for msg in consumer:
    query = msg.value

    print("\n==============================")
    print("📩 QUERY:", query)

    try:
        process(query)
        print("✅ SUCCESS")

    except Exception as e:
        print("❌ ERROR:", e)

        retry = int(query.get("_retry_count", 0))

        # IMPORTANTE: incrementar contador
        query["_retry_count"] = retry + 1

        producer.send("queries.retry", query)
        producer.flush()

        print(f"🔁 SENT TO RETRY (count={query['_retry_count']})")

    finally:
        print("⏱ DONE")