from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
import random
import time
import json
import uuid
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "queries"

TOTAL_MESSAGES = 60
FAIL_DLQ_RANGE = 10
FAIL_RETRY_RANGE = 10

zones = ["Z1", "Z2", "Z3", "Z4", "Z5"]


def build_query(i):
    zone = random.choice(zones)

    base = {
        "query_id": str(uuid.uuid4()),
        "zone_id": zone,
        "confidence_min": 0.8,
        "_retry_count": 0
    }

    # 🔴 DLQ garantizado
    if i == TOTAL_MESSAGES - 1:
        base.update({
            "query_type": "Q_FAIL_DLQ",
            "force_fail": "DLQ"
        })
        return base

    # 🟡 retry fail zone
    if i >= TOTAL_MESSAGES - (FAIL_DLQ_RANGE + FAIL_RETRY_RANGE):
        base.update({
            "query_type": "Q_FAIL_RETRY",
            "force_fail": "RETRY"
        })
        return base

    # 🟢 normal
    base.update({
        "query_type": random.choice(["Q1", "Q2", "Q3", "Q4", "Q5"]),
        "force_fail": None
    })

    return base


def connect_kafka():
    for i in range(30):  # ⬅️ más intentos
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=10,
                request_timeout_ms=20000
            )
            print("✅ Kafka conectado")
            return producer

        except (NoBrokersAvailable, Exception) as e:
            print(f"⏳ Kafka no listo ({i+1}/30): {e}")
            time.sleep(3)

    raise Exception("Kafka no disponible")


def run():
    print("🚀 Traffic V2 iniciado")

    producer = connect_kafka()

    for i in range(TOTAL_MESSAGES):
        msg = build_query(i)

        try:
            future = producer.send(TOPIC, msg)
            future.get(timeout=10)  # ⬅️ asegura envío real
        except KafkaTimeoutError:
            print(f"[{i}] ❌ timeout enviando mensaje")
        except Exception as e:
            print(f"[{i}] ❌ error enviando mensaje: {e}")

        if msg["force_fail"] == "DLQ":
            print(f"[{i}] 🔴 DLQ (FORZADO FINAL)")
        elif msg["force_fail"] == "RETRY":
            print(f"[{i}] 🟡 RETRY")
        else:
            print(f"[{i}] 🟢 normal")

        time.sleep(0.1)

    producer.flush()
    print("🏁 FIN TRAFFIC")


if __name__ == "__main__":
    run()