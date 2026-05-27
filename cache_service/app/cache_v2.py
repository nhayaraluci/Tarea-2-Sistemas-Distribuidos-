from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import requests
import hashlib
import json
import time
from collections import OrderedDict, defaultdict

import matplotlib.pyplot as plt
import io
import pickle

app = FastAPI()

POLICY = "LRU" ##LRU


MAX_SIZE = 500 * 1024 * 1024
TTL = 50

MAX_LAT_SAMPLES = 500


CACHE = OrderedDict()
FREQ = defaultdict(int)

CACHE_SIZE = 0

metrics = {
    "hits": 0,
    "misses": 0,
    "evictions": 0,
    "latencies": []
}

traffic_log = []

RESPONSE_GENERATOR_URL = "http://response-generator:8001/query"


def get_size(obj):
    return len(pickle.dumps(obj))


class QueryRequest(BaseModel):
    query_type: str
    zone_id: str | None = None
    zone_id_a: str | None = None
    zone_id_b: str | None = None
    confidence_min: float = 0.0
    bins: int = 5


def generate_cache_key(request: QueryRequest):
    return hashlib.md5(
        json.dumps(request.dict(), sort_keys=True).encode()
    ).hexdigest()


def evict():
    global CACHE, CACHE_SIZE

    if POLICY == "LRU":
        key, (data, ts) = CACHE.popitem(last=False)
        CACHE_SIZE -= get_size((data, ts))

    elif POLICY == "LFU":
        lfu_key = min(FREQ, key=FREQ.get)
        data_to_evict = CACHE[lfu_key] # Obtener el objeto
        CACHE_SIZE -= get_size(data_to_evict) # Restar su tamaño
        CACHE.pop(lfu_key, None)
        FREQ.pop(lfu_key, None)


@app.post("/query")
def query(request: QueryRequest):
    global CACHE_SIZE

    start = time.time()
    key = generate_cache_key(request)

    # ---------- HIT ----------
    if key in CACHE:
        data, ts = CACHE[key]

        if time.time() - ts < TTL:
            metrics["hits"] += 1

            if POLICY == "LRU":
                CACHE.move_to_end(key)
            elif POLICY == "LFU":
                FREQ[key] += 1

            lat = time.time() - start
            metrics["latencies"].append(lat)

            traffic_log.append(1)
            return {"source": "cache", "data": data}

        CACHE.pop(key, None)
        FREQ.pop(key, None)

  
    metrics["misses"] += 1

    try:
        response = requests.post(
            RESPONSE_GENERATOR_URL,
            json=request.dict()
        )

        if response.status_code != 200:
            raise HTTPException(status_code=500, detail=response.text)

        data = response.json()
        entry = (data, time.time())
        entry_size = get_size(entry)

        if CACHE_SIZE + entry_size > MAX_SIZE:
            evict()
            metrics["evictions"] += 1

        CACHE[key] = entry
        CACHE_SIZE += entry_size

        if POLICY == "LFU":
            FREQ[key] = 1

        lat = time.time() - start
        metrics["latencies"].append(lat)

        traffic_log.append(0)

        return {"source": "computed", "data": data}

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/metrics")
def metrics_view():
    total = metrics["hits"] + metrics["misses"]
    hit_rate = metrics["hits"] / total if total else 0

    return {
        "policy": POLICY,
        "ttl_seconds": TTL,
        "cache_size_mb": CACHE_SIZE / (1024 * 1024),
        "hits": metrics["hits"],
        "misses": metrics["misses"],
        "hit_rate": hit_rate,
        "evictions": metrics["evictions"]
    }



@app.delete("/cache")
def clear():
    global CACHE_SIZE

    CACHE.clear()
    FREQ.clear()
    CACHE_SIZE = 0

    metrics["hits"] = 0
    metrics["misses"] = 0
    metrics["evictions"] = 0
    metrics["latencies"] = []
    traffic_log.clear()

    return {"message": "cache limpiado"}



@app.get("/plot")
def plot():
    if not traffic_log:
        return {"message": "No hay datos aún"}

    x = list(range(len(traffic_log)))
    y = traffic_log

    plt.figure()
    plt.scatter(x, y, alpha=0.6)

    cache_mb = CACHE_SIZE / (1024 * 1024)

    plt.title(
        f"Cache performance ({POLICY}) | TTL={TTL}s | Size={cache_mb:.2f} MB"
    )

    plt.xlabel("Request order")
    plt.ylabel("Hit / Miss")
    plt.yticks([0, 1], ["Miss", "Hit"])

    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    plt.close()
    buf.seek(0)

    return StreamingResponse(buf, media_type="image/png")