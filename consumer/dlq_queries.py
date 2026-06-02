from kafka import KafkaConsumer
import json
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

consumer = KafkaConsumer(
    "queries.dlq",
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    group_id="dlq-worker-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("💀 DLQ WORKER INICIADO")

for msg in consumer:
    data = msg.value

    print("\n" + "=" * 60)
    print("💀 DLQ MESSAGE CAPTURED")
    print(json.dumps(data, indent=2))
    print("=" * 60)