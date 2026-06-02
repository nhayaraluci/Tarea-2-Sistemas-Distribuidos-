from fastapi import FastAPI
from pydantic import BaseModel
import time
import hashlib
import json
from collections import OrderedDict

app = FastAPI()

CACHE = OrderedDict()
TTL = 50

metrics = {
    "hits": 0,
    "misses": 0
}


class QueryRequest(BaseModel):
    query_type: str
    zone_id: str | None = None
    zone_id_a: str | None = None
    zone_id_b: str | None = None
    confidence_min: float = 0.0
    bins: int = 5


def key(req):
    return hashlib.md5(json.dumps(req.dict(), sort_keys=True).encode()).hexdigest()


@app.post("/query")
def query(req: QueryRequest):

    k = key(req)

    print("\n==============================")
    print(f"[CACHE] query_id = {req.query_type}")
    print(f"[CACHE] key = {k}")

    # HIT
    if k in CACHE:
        data, ts = CACHE[k]

        if time.time() - ts < TTL:
            metrics["hits"] += 1

            print("[CACHE HIT] ✔")

            return {
                "source": "cache",
                "data": data
            }

        CACHE.pop(k)

    # MISS
    metrics["misses"] += 1

    print("[CACHE MISS] ❌ → computing...")

    # simula response-generator
    data = {
        "result": "computed",
        "query": req.dict()
    }

    CACHE[k] = (data, time.time())

    print("[CACHE STORED] ✔")

    return {
        "source": "computed",
        "data": data
    }


@app.get("/metrics")
def metrics_view():
    total = metrics["hits"] + metrics["misses"]

    return {
        "hits": metrics["hits"],
        "misses": metrics["misses"],
        "hit_rate": metrics["hits"] / total if total else 0
    }