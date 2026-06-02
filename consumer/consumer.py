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

print("\nMAIN CONSUMER STARTED\n")


def process(query):

    if query.get("force_fail") == "DLQ":
        raise Exception("FORCED DLQ ERROR")

    if query.get("force_fail") == "RETRY":
        raise Exception("FORCED RETRY ERROR")

    if random.random() < 0.1:
        raise Exception("RANDOM FAILURE")

    resp = requests.post(
        CACHE_URL,
        json=query,
        timeout=5
    )

    if resp.status_code >= 500:
        raise Exception("CACHE ERROR")

    return resp.json()

for msg in consumer:
    query = msg.value

    print("\n----------------------------------------")
    print("NEW QUERY")
    print(f"ID      : {query.get('query_id')}")
    print(f"TYPE    : {query.get('query_type')}")
    print(f"ZONE    : {query.get('zone_id')}")
    print("----------------------------------------")

    try:
        process(query)

        print("RESULT  : SUCCESS")

    except Exception as e:

        retry = int(query.get("_retry_count", 0)) + 1
        query["_retry_count"] = retry

        producer.send("queries.retry", query)
        producer.flush()

        print("RESULT  : FAILED")
        print(f"ERROR   : {e}")
        print("ACTION  : SEND TO RETRY")
        print(f"ATTEMPT : {retry}/3")

    print("----------------------------------------")