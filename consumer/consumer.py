from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import requests
import json
import time
import os

CACHE_URL = "http://cache-service:8000/query"

TOPIC_MAIN = "queries"
TOPIC_RETRY = "queries.retry"
TOPIC_DLQ = "queries.dlq"

GROUP_ID = "cache-consumer-group"

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "3"))


def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers="kafka:9092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Producer interno conectado a Kafka")
            return producer

        except NoBrokersAvailable:
            print("Kafka no listo para producer, reintentando en 3s...")
            time.sleep(3)


def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_MAIN,
                TOPIC_RETRY,
                bootstrap_servers="kafka:9092",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                group_id=GROUP_ID,
                value_deserializer=lambda x: json.loads(x.decode("utf-8"))
            )
            print("Consumer conectado a Kafka")
            return consumer

        except NoBrokersAvailable:
            print("Kafka no listo para consumer, reintentando en 3s...")
            time.sleep(3)


def send_to_retry(producer, query, error_message):
    retry_count = int(query.get("_retry_count", 0)) + 1

    retry_message = {
        **query,
        "_retry_count": retry_count,
        "_last_error": error_message,
        "_origin_topic": TOPIC_MAIN
    }

    print(f"Enviando a retry intento {retry_count}/{MAX_RETRIES}: {retry_message}")

    time.sleep(RETRY_DELAY_SECONDS)

    producer.send(TOPIC_RETRY, retry_message)
    producer.flush()


def send_to_dlq(producer, query, error_message):
    dlq_message = {
        **query,
        "_final_error": error_message,
        "_retry_count": int(query.get("_retry_count", 0)),
        "_origin_topic": TOPIC_MAIN
    }

    print(f"Enviando a DLQ: {dlq_message}")

    producer.send(TOPIC_DLQ, dlq_message)
    producer.flush()


def process_query(query):
    response = requests.post(
        CACHE_URL,
        json=query,
        timeout=5
    )

    if 400 <= response.status_code < 500:
        raise ValueError(f"Error de cliente {response.status_code}: {response.text}")

    response.raise_for_status()

    return response.json()


def should_send_to_dlq_directly(error):
    error_text = str(error)

    if "Error de cliente 400" in error_text:
        return True

    if "Error de cliente 401" in error_text:
        return True

    if "Error de cliente 403" in error_text:
        return True

    if "Error de cliente 404" in error_text:
        return True

    return False


producer = create_producer()
consumer = create_consumer()

print("Consumer iniciado con retry y DLQ")
print(f"Topicos escuchados: {TOPIC_MAIN}, {TOPIC_RETRY}")
print(f"Max retries: {MAX_RETRIES}")
print(f"DLQ: {TOPIC_DLQ}")
print()

for message in consumer:
    query = message.value

    print(f"Query recibida desde {message.topic}: {query}")

    try:
        result = process_query(query)
        print("Respuesta OK:", result)

    except Exception as error:
        error_message = str(error)
        retry_count = int(query.get("_retry_count", 0))

        print("Error procesando query:", error_message)

        if should_send_to_dlq_directly(error):
            send_to_dlq(producer, query, error_message)

        elif retry_count < MAX_RETRIES:
            send_to_retry(producer, query, error_message)

        else:
            send_to_dlq(producer, query, error_message)

    finally:
        consumer.commit()
        time.sleep(0.1)