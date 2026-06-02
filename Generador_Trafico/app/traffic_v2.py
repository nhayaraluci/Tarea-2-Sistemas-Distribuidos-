from kafka import KafkaProducer
import random
import time
import json
import uuid

# =========================
# ESPERAR KAFKA
# =========================
def wait_kafka():
    print("⏳ Esperando Kafka...")

    for i in range(30):
        try:
            KafkaProducer(
                bootstrap_servers="kafka:9092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=5000
            )
            print("✅ Kafka listo")
            return
        except Exception:
            print(f"Kafka no disponible... intento {i+1}/30")
            time.sleep(3)

    raise Exception("❌ Kafka no arrancó")


wait_kafka()

# =========================
# PRODUCER
# =========================
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5
)

zones = ["Z1", "Z2", "Z3", "Z4", "Z5"]
query_types = ["Q1", "Q2", "Q3", "Q4", "Q5"]
confidence_values = [0.0, 0.5, 0.8]


def build_query():
    qtype = random.choice(query_types)
    zone = random.choice(zones)

    base = {
        "query_id": str(uuid.uuid4()),
        "timestamp": time.time(),
        "query_type": qtype,
        "confidence_min": random.choice(confidence_values),
    }

    if qtype == "Q4":
        base["zone_id_a"] = zone
        base["zone_id_b"] = random.choice(zones)
    else:
        base["zone_id"] = zone

    if qtype == "Q5":
        base["bins"] = 5

    return base


def run():
    print("🚀 Traffic generator iniciado")

    for i in range(50):
        query = build_query()

        producer.send("queries", query)
        producer.flush()

        print(f"[{i+1}] enviada -> {query}")

        time.sleep(0.1)

    print("🏁 FIN TRAFFIC")


if __name__ == "__main__":
    run()