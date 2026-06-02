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

print("\nRETRY WORKER STARTED\n")


def send_dlq(query):

    query = dict(query)
    query["_status"] = "DLQ"

    producer.send(TOPIC_DLQ, query)
    producer.flush()


def process(query):

    force = query.get("force_fail")

    if force == "DLQ":
        raise Exception("FORCED DLQ ERROR")

    if force == "RETRY":
        raise Exception("FORCED RETRY ERROR")

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
    retry_count = int(query.get("_retry_count", 0))

    print("\n----------------------------------------")
    print("RETRY PROCESS")
    print(f"ID      : {query.get('query_id')}")
    print(f"TYPE    : {query.get('query_type')}")
    print(f"ZONE    : {query.get('zone_id')}")
    print(f"ATTEMPT : {retry_count}/3")
    print("----------------------------------------")

    try:

        process(query)

        print("RESULT  : SUCCESS")

    except Exception as e:

        retry_count += 1
        query["_retry_count"] = retry_count

        print("RESULT  : FAILED")
        print(f"ERROR   : {e}")

        if retry_count >= MAX_RETRIES:

            print("ACTION  : SEND TO DLQ")
            print(f"FINAL   : {retry_count}/3")

            send_dlq(query)

        else:

            print("ACTION  : RETRY AGAIN")
            print(f"NEXT    : {retry_count}/3")

            producer.send(TOPIC_RETRY, query)
            producer.flush()

    print("----------------------------------------")