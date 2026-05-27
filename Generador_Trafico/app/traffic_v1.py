import requests
import random
import time
import matplotlib.pyplot as plt

URL = "http://cache-service:8000/query"

ZONES = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERIES = ["Q1", "Q2", "Q3", "Q4", "Q5"]


def random_query():
    q = random.choice(QUERIES)

    if q == "Q4":
        return {
            "query_type": q,
            "zone_id_a": random.choice(ZONES),
            "zone_id_b": random.choice(ZONES),
            "confidence_min": round(random.random(), 2)
        }

    elif q == "Q5":
        return {
            "query_type": q,
            "zone_id": random.choice(ZONES),
            "bins": random.randint(3, 10)
        }

    else:
        return {
            "query_type": q,
            "zone_id": random.choice(ZONES),
            "confidence_min": round(random.random(), 2)
        }


def run(mode="uniform", n_requests=100):
    print("🚦 Traffic generator iniciado\n")

    hits = 0
    misses = 0

    x_points = []
    y_points = []

    for i in range(n_requests):
        payload = random_query()

        try:
            r = requests.post(URL, json=payload)

            if r.status_code != 200:
                print(f"{i+1}/{n_requests} ERROR {r.status_code}")
                continue

            data = r.json()

            if data.get("source") == "cache":
                hits += 1
                y_points.append(1)
                label = "HIT"
            else:
                misses += 1
                y_points.append(0)
                label = "MISS"

            x_points.append(i)

            print(f"{i+1}/{n_requests} OK → {label}")

        except Exception as e:
            print("Error:", e)

        time.sleep(0.1)

    total = hits + misses

    print("\n📊 Resultados:")
    print(f"Total requests: {total}")
    print(f"Hits: {hits}")
    print(f"Misses: {misses}")
    print(f"Hit Rate: {hits / total:.2f}" if total > 0 else "Hit Rate: 0")

    print("\n📊 Generando gráfico...\n")
    draw(x_points, y_points)


def draw(x_points, y_points):
    plt.figure()

    plt.scatter(x_points, y_points, alpha=0.6)

    plt.title(f"Cache behavior ({len(x_points)} requests)")
    plt.xlabel("Query index")
    plt.ylabel("Result")

    plt.yticks([0, 1], ["Miss", "Hit"])

    plt.savefig("/app/metrics.png")
    print("Gráfico guardado en /app/metrics.png")


if __name__ == "__main__":
    run("uniform")