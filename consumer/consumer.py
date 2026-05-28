from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import requests
import json
import time

CACHE_URL = "http://cache-service:8000/query"

# -----------------------------
# 1. CONEXIÓN ROBUSTA A KAFKA
# -----------------------------
consumer = None

while True:
    try:
        consumer = KafkaConsumer(
            "queries",
            bootstrap_servers="kafka:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="cache-consumer-group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )
        print("✔ Consumer conectado a Kafka")
        break

    except NoBrokersAvailable:
        print("⏳ Kafka no listo aún, reintentando en 3s...")
        time.sleep(3)

# -----------------------------
# 2. PROCESAMIENTO DE MENSAJES
# -----------------------------
print("🚀 Consumer iniciado...\n")

for message in consumer:

    try:
        query = message.value
        print(f"📩 Query recibida: {query}")

        response = requests.post(
            CACHE_URL,
            json=query,
            timeout=5
        )

        try:
            print("✅ Respuesta:", response.json())
        except Exception:
            print("⚠️ Respuesta no es JSON:", response.text)

    except requests.exceptions.RequestException as e:
        print("❌ Error HTTP con cache-service:", e)

    except Exception as e:
        print("❌ Error general:", e)

    time.sleep(0.1)