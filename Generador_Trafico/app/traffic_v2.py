from kafka import KafkaProducer
import random
import time
import json
import os

# =========================
# ESPERAR A KAFKA
# =========================
print("Esperando Kafka...")

producer = None

while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("Producer conectado a Kafka ✔")
    except Exception as e:
        print("Kafka no listo aún, reintentando...", e)
        time.sleep(3)

# =========================
# CONFIGURACIÓN
# =========================
zones = ["Z1", "Z2", "Z3", "Z4", "Z5"]
query_types = ["Q1", "Q2", "Q3", "Q4", "Q5"]
confidence_values = [0.0, 0.5, 0.8]


# =========================
# CONSTRUCTOR DE CONSULTAS
# =========================
def build_query(query_type, zone_id, confidence_min):
    if query_type == "Q4":
        zone_id_a = zone_id
        zone_id_b = random.choice([z for z in zones if z != zone_id_a])

        return {
            "query_type": "Q4",
            "zone_id_a": zone_id_a,
            "zone_id_b": zone_id_b,
            "confidence_min": confidence_min
        }

    if query_type == "Q5":
        return {
            "query_type": "Q5",
            "zone_id": zone_id,
            "confidence_min": confidence_min,
            "bins": 5
        }

    return {
        "query_type": query_type,
        "zone_id": zone_id,
        "confidence_min": confidence_min
    }


# =========================
# GENERADORES
# =========================
def generate_uniform():
    query_type = random.choice(query_types)
    zone_id = random.choice(zones)
    confidence_min = random.choice(confidence_values)

    return build_query(query_type, zone_id, confidence_min)


def generate_zipf():
    weights = [0.5, 0.2, 0.15, 0.1, 0.05]

    query_type = random.choice(query_types)
    zone_id = random.choices(zones, weights=weights)[0]
    confidence_min = random.choice(confidence_values)

    return build_query(query_type, zone_id, confidence_min)


# =========================
# MAIN LOOP
# =========================
def run(mode="uniform"):
    print("🚦 Kafka Producer iniciado\n")

    total_queries = 50

    for i in range(total_queries):
        if mode == "zipf":
            query = generate_zipf()
        else:
            query = generate_uniform()

        try:
            producer.send("queries", query)
            print(f"{i + 1}/{total_queries} enviada → {query}")
        except Exception as e:
            print("Error enviando query a Kafka:", e)

        time.sleep(0.1)

    producer.flush()
    print("\n✅ Todas las consultas fueron enviadas a Kafka")


if __name__ == "__main__":
    mode = os.getenv("MODE", "uniform")
    run(mode)