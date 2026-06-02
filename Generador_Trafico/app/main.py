import os
import time
import requests


def wait_for_cache():
    print("Esperando cache-service...")

    url = "http://cache-service:8000/metrics"

    for i in range(20):
        try:
            requests.get(url, timeout=2)
            print("Cache listo")
            return
        except Exception:
            print(f"Intento {i + 1}/20... cache aún no responde")
            time.sleep(2)

    print("Cache no disponible, continúo igual")


def load_runner():
    from app.traffic_v2 import run
    return run


if __name__ == "__main__":
    print("Traffic generator iniciado")

    wait_for_cache()

    run = load_runner()

    # 🔥 IMPORTANTE: sin parámetros
    run()

    print("FIN MAIN")