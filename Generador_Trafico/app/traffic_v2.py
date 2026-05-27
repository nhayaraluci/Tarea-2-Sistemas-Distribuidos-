from kafka import KafkaProducer
import random
import time
import json

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

zones = ["Z1", "Z2", "Z3", "Z4", "Z5"]
query_types = ["Q1", "Q2", "Q3", "Q4", "Q5"]


def generate_uniform():
    return {
        "query_type": random.choice(query_types),
        "zone_id": random.choice(zones),
        "confidence_min": random.choice([0.0, 0.5, 0.8]),
        "bins": 5
    }


def generate_zipf():
    weights = [0.5, 0.2, 0.15, 0.1, 0.05]

    zone = random.choices(zones, weights=weights)[0]

    return {
        "query_type": random.choice(query_types),
        "zone_id": zone,
        "confidence_min": 0.0,
        "bins": 5
    }


def run(mode="uniform"):

    print("🚦 Kafka Producer iniciado\n")

    for i in range(600):

        query = generate_zipf() if mode == "zipf" else generate_uniform()

        try:

            producer.send("queries", query)

            print(f"{i+1}/600 enviada → {query}")

        except Exception as e:
            print("error:", e)

        time.sleep(0.1)

    print("\n✅ Todas las consultas fueron enviadas a Kafka")


if __name__ == "__main__":
    run("zipf")  # o "uniform"