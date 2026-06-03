from fastapi import FastAPI
from pydantic import BaseModel
from collections import OrderedDict
import hashlib
import json
import time
import requests
import os

app = FastAPI()

# =========================
# CONFIG
# =========================

RESPONSE_URL = os.getenv(
    "RESPONSE_URL",
    "http://response-generator:8000/query"
)

TTL = 50

# =========================
# CACHE
# =========================

CACHE = OrderedDict()

# =========================
# MÉTRICAS
# =========================

metrics = {
    "hits": 0,
    "misses": 0
}

response_times = []


class QueryRequest(BaseModel):
    query_type: str
    zone_id: str | None = None
    zone_id_a: str | None = None
    zone_id_b: str | None = None
    confidence_min: float = 0.0
    bins: int = 5


def generate_key(req):
    return hashlib.md5(
        json.dumps(
            req.dict(),
            sort_keys=True
        ).encode()
    ).hexdigest()


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

            metrics["hits"] += 1

            latency = time.time() - start
            response_times.append(latency)

            print("[CACHE HIT]")

            return {
                "source": "cache",
                "latency": latency,
                "data": data
            }

        print("[CACHE EXPIRED]")
        CACHE.pop(key)

    # =========================
    # CACHE MISS
    # =========================

    metrics["misses"] += 1

    print("[CACHE MISS]")
    print("[CALLING RESPONSE GENERATOR]")

    response = requests.post(
        RESPONSE_URL,
        json=req.dict(),
        timeout=10
    )

    response.raise_for_status()

    data = response.json()

    CACHE[key] = (
        data,
        time.time()
    )

    print("[CACHE STORED]")

    latency = time.time() - start
    response_times.append(latency)

    return {
        "source": "response_generator",
        "latency": latency,
        "data": data
    }


@app.get("/metrics")
def metrics_view():

    total = metrics["hits"] + metrics["misses"]

    avg_latency = (
        sum(response_times) / len(response_times)
        if response_times else 0
    )

    return {
        "hits": metrics["hits"],
        "misses": metrics["misses"],
        "hit_rate": metrics["hits"] / total if total else 0,
        "avg_latency": avg_latency,
        "cache_size": len(CACHE),
        "ttl": TTL
    }