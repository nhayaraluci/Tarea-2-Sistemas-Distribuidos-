from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import os
import time

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

consumer = None

while consumer is None:

    try:

        print("CONNECTING TO KAFKA...")

        consumer = KafkaConsumer(
            "queries.dlq",
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="dlq-worker",
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )

        print("DLQ WORKER STARTED")

    except NoBrokersAvailable:

        print("KAFKA NOT READY - RETRYING IN 5 SECONDS")
        time.sleep(5)

for msg in consumer:

    query = msg.value

    print("\n" + "=" * 60)
    print("DEAD LETTER QUEUE MESSAGE")
    print("=" * 60)

    print(f"QUERY ID     : {query.get('query_id')}")
    print(f"QUERY TYPE   : {query.get('query_type')}")
    print(f"ZONE         : {query.get('zone_id')}")
    print(f"RETRY COUNT  : {query.get('_retry_count')}")
    print(f"STATUS       : {query.get('_status')}")

    print("\nFAILED MESSAGE:")
    print(json.dumps(query, indent=4))

    print("=" * 60)