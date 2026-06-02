from kafka import KafkaConsumer, KafkaProducer
import requests
import json
import os
import time

time.sleep(10)

# =========================
# CONFIG
# =========================

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
CACHE_URL = os.getenv("CACHE_URL", "http://cache-service:8000/query")

TOPIC_MAIN = "queries"
TOPIC_RETRY = "queries.retry"
TOPIC_DLQ = "queries.dlq"

MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_DELAY = float(os.getenv("RETRY_DELAY_SECONDS", 2))

# =========================
# PRODUCER
# =========================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5
)

# =========================
# CONSUMER (FIX IMPORTANTE)
# =========================

consumer = KafkaConsumer(
    TOPIC_MAIN,
    TOPIC_RETRY,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",   # 🔥 FIX CLAVE
    enable_auto_commit=False,
    group_id="cache-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=1000        # 🔥 DEBUG útil
)

print("✅ Consumer iniciado y escuchando Kafka...")

# =========================
# HELPERS
# =========================

def send_retry(query):
    query["_retry_count"] = query.get("_retry_count", 0) + 1
    producer.send(TOPIC_RETRY, query)
    producer.flush()
    print(f"🔁 RETRY {query['_retry_count']}")

def send_dlq(query):
    producer.send(TOPIC_DLQ, query)
    producer.flush()
    print("💀 DLQ")

def process(query):
    resp = requests.post(CACHE_URL, json=query, timeout=5)

    if resp.status_code >= 500:
        raise Exception(f"Cache error {resp.status_code}")

    return resp.json()

# =========================
# LOOP PRINCIPAL (DEBUG REAL)
# =========================

print("🔥 Esperando mensajes...\n")

while True:
    msg_pack = consumer.poll(timeout_ms=1000)

    if not msg_pack:
        print("⏳ sin mensajes...")
        continue

    for tp, messages in msg_pack.items():
        for msg in messages:

            print("🔥 LOOP ACTIVO")

            query = msg.value

            print("\n==============================")
            print("📩 Mensaje recibido:", query)

            try:
                result = process(query)
                print("✅ OK:", result)

            except Exception as e:
                print("❌ ERROR:", e)

                retry_count = query.get("_retry_count", 0)

                if retry_count < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                    send_retry(query)
                else:
                    send_dlq(query)

            finally:
                try:
                    consumer.commit()
                except Exception as e:
                    print("⚠️ Commit error (ignorado):", e)

                time.sleep(0.05)