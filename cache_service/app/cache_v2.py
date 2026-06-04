from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from collections import OrderedDict
import hashlib
import json
import time
import requests
import os

app = FastAPI()

# =========================
# CONFIGURACIÓN CACHE
# =========================

RESPONSE_URL = os.getenv(
    "RESPONSE_URL",
    "http://response-generator:8001/query"
)

TTL = 50
MAX_CACHE_SIZE = 100

# =========================
# CACHE
# =========================

CACHE = OrderedDict()

# =========================
# MÉTRICAS COMPLETAS
# =========================

metrics = {
    "hits": 0,
    "misses": 0,
    "evictions": 0,
    "controlled_errors": 0  # 🔴 Cuenta los fallos simulados o caídas del generador
}

response_times = []


class QueryRequest(BaseModel):
    query_type: str
    zone_id: str | None = None
    zone_id_a: str | None = None
    zone_id_b: str | None = None
    confidence_min: float = 0.0
    bins: int = 5
    force_fail: str | bool | None = None  # 🟢 Permite recibir los flags de falla de tu traffic


def generate_key(req):
    # Excluimos force_fail de la clave de la caché para que no altere el comportamiento normal
    req_dict = req.dict()
    req_dict.pop("force_fail", None)
    return hashlib.md5(
        json.dumps(req_dict, sort_keys=True).encode()
    ).hexdigest()


def evict_if_needed():
    while len(CACHE) > MAX_CACHE_SIZE:
        CACHE.popitem(last=False)
        metrics["evictions"] += 1


@app.post("/query")
def query(req: QueryRequest):

    start = time.time()
    key = generate_key(req)

    print("\n==============================")
    print(f"[CACHE] QUERY = {req.query_type}")
    print(f"[CACHE] KEY   = {key}")

    # =========================
    # CACHE HIT
    # =========================
    if key in CACHE:
        data, timestamp = CACHE[key]

        if time.time() - timestamp < TTL:
            CACHE.move_to_end(key)  # 🔥 LRU update

            metrics["hits"] += 1
            latency = (time.time() - start) * 1000  # Convertimos a milisegundos (ms)
            response_times.append(latency)

            print("[CACHE HIT]")

            return {
                "source": "cache",
                "latency": latency,
                "data": data
            }

        CACHE.pop(key)

    # =========================
    # CACHE MISS
    # =========================
    metrics["misses"] += 1
    print("[CACHE MISS]")
    print("[CALLING RESPONSE GENERATOR]")

    try:
        # Hacemos la llamada real al generador de respuestas
        response = requests.post(
            RESPONSE_URL,
            json=req.dict(),
            timeout=5
        )
        response.raise_for_status()
        data = response.json()

        CACHE[key] = (data, time.time())
        evict_if_needed()

        latency = (time.time() - start) * 1000  # Convertimos a milisegundos (ms)
        response_times.append(latency)
        print("[CACHE STORED]")

        return {
            "source": "response_generator",
            "latency": latency,
            "data": data
        }

    except Exception as e:
        # 🟢 CONTROL DE ERRORES: Captura las fallas de red o consultas falsas (Q_FAIL_RETRY)
        metrics["controlled_errors"] += 1
        latency = (time.time() - start) * 1000
        response_times.append(latency)
        
        print(f"\n[CACHE CONTROLLED ERROR] Fallo esperado con {req.query_type}: {e}")
        
        # Devolvemos un código 502 limpio. El consumidor lo captura, no salen flechas ^^^, 
        # y Kafka lo manda de inmediato a reintento de forma correcta.
        raise HTTPException(
            status_code=502, 
            detail=f"Error controlado en la ruta hacia el generador de respuestas: {str(e)}"
        )


@app.get("/metrics")
def metrics_view():
    total = metrics["hits"] + metrics["misses"]
    
    # Ordenamos los tiempos para poder calcular los percentiles reales de tu tabla
    sorted_times = sorted(response_times)
    n = len(sorted_times)
    
    p50 = 0
    p95 = 0
    if n > 0:
        # Percentil 50 (Mediana)
        p50 = sorted_times[int(n * 0.50)]
        # Percentil 95 (Casos críticos/fallas)
        p95 = sorted_times[int(n * 0.95) if int(n * 0.95) < n else n - 1]

    avg_latency = sum(response_times) / n if n else 0

    return {
        "hits": metrics["hits"],
        "misses": metrics["misses"],
        "hit_rate": metrics["hits"] / total if total else 0,
        "controlled_errors": metrics["controlled_errors"],
        "avg_latency_ms": avg_latency,
        "latencia_p50_ms": p50,
        "latencia_p95_ms": p95,
        "evictions": metrics["evictions"],
        "cache_size": len(CACHE),
        "ttl": TTL,
        "max_cache_size": MAX_CACHE_SIZE
    }