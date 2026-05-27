from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import hashlib
import json
import time

app = FastAPI()

CACHE = {}

hits = 0
misses = 0

RESPONSE_GENERATOR_URL = "http://response-generator:8001/query"


class QueryRequest(BaseModel):
    query_type: str
    zone_id: str | None = None
    zone_id_a: str | None = None
    zone_id_b: str | None = None
    confidence_min: float = 0.0
    bins: int = 5


def generate_cache_key(request: QueryRequest):
    request_dict = request.dict()
    request_str = json.dumps(request_dict, sort_keys=True)
    return hashlib.md5(request_str.encode()).hexdigest()


@app.post("/query")
def query(request: QueryRequest):
    global hits, misses

    start = time.time()
    cache_key = generate_cache_key(request)

    if cache_key in CACHE:
        hits += 1
        latency = time.time() - start
        return {
            "source": "cache",
            "latency": latency,
            "data": CACHE[cache_key]
        }

    misses += 1

    try:
        response = requests.post(
            RESPONSE_GENERATOR_URL,
            json=request.dict()
        )

        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=response.text
            )

        data = response.json()
        CACHE[cache_key] = data

        latency = time.time() - start

        return {
            "source": "computed",
            "latency": latency,
            "data": data
        }

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health():
    return {
        "status": "ok",
        "cache_size": len(CACHE)
    }


@app.get("/metrics")
def metrics():
    total = hits + misses
    hit_rate = hits / total if total > 0 else 0

    return {
        "hits": hits,
        "misses": misses,
        "hit_rate": hit_rate,
        "cache_size": len(CACHE)
    }


@app.delete("/cache")
def clear_cache():
    global CACHE, hits, misses
    CACHE.clear()
    hits = 0
    misses = 0

    return {
        "message": "Cache limpiado",
        "hits": hits,
        "misses": misses
    }