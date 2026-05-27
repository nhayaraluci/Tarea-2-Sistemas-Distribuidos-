from kafka import KafkaProducer
import random
import time
import json

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

ZONES = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERIES = ["Q1", "Q2", "Q3", "Q4", "Q5"]


def random_query():
    q = random.choice(QUERIES)

    if q == "Q4":
        return {
            "query_type": q,
            "zone_id_a": random.choice(ZONES),
            "zone_id_b": random.choice(ZONES),
            "confidence_min": round(random.random(), 2)
        }

    elif q == "Q5":
        return {
            "query_type": q,
            "zone_id": random.choice(ZONES),
            "bins": random.randint(3, 10)
        }

    else:
        return {
            "query_type": q,
            "zone_id": random.choice(ZONES),
            "confidence_min": round(random.random(), 2)
        }


def run(mode="uniform", n_requests=100):

    print("🚦 Kafka Producer iniciado\n")

    for i in range(n_requests):

        payload = random_query()

        try:
            producer.send("queries", payload)

            print(f"{i+1}/{n_requests} enviada → {payload}")

        except Exception as e:
            print("Error:", e)

        time.sleep(0.1)


if __name__ == "__main__":
    run("uniform")