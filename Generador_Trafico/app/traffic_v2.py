import requests
import random
import time
import matplotlib.pyplot as plt

CACHE_URL = "http://cache-service:8000/query"

zones = ["Z1", "Z2", "Z3", "Z4", "Z5"]
query_types = ["Q1", "Q2", "Q3", "Q4", "Q5"]


def generate_uniform():
    return {
        "query_type": random.choice(query_types),
        "zone_id": random.choice(zones),
        "confidence_min": random.choice([0.0, 0.5, 0.8]),
        "bins": 5
    }


def generate_zipf():
    weights = [0.5, 0.2, 0.15, 0.1, 0.05]
    zone = random.choices(zones, weights=weights)[0]

    return {
        "query_type": random.choice(query_types),
        "zone_id": zone,
        "confidence_min": 0.0,
        "bins": 5
    }


def run(mode="uniform"):
    hits = 0
    misses = 0

    x_points = []
    y_points = []

    for i in range(600):

        query = generate_zipf() if mode == "zipf" else generate_uniform()

        try:
            r = requests.post(CACHE_URL, json=query)

            if r.status_code != 200:
                print(f"{i+1}/100 ERROR {r.status_code}")
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

            print(f"{i+1}/100 OK → {label}")

        except Exception as e:
            print("error:", e)

        time.sleep(0.1)

    print(f"\nTotal Hits: {hits}")
    print(f"Total Misses: {misses}")
    print(f"Hit Rate: {hits / (hits + misses):.2f}")

    draw(x_points, y_points)


def draw(x_points, y_points):
    plt.figure()

    plt.scatter(x_points, y_points, alpha=0.6)

    plt.title(f"Cache performance ({len(x_points)} requests)")
    plt.xlabel("Request")
    plt.ylabel("Hit/Miss")

    plt.yticks([0, 1], ["Miss", "Hit"])

    plt.savefig("/app/metrics.png")
    print("Gráfico guardado en /app/metrics.png")


if __name__ == "__main__":
    run("zipf")  # cambia a "uniform" si quieres o zipf