import os

version = os.getenv("VERSION", "v1")

if version == "v2":
    from app.cache_v2 import app
else:
    from app.cache_v1 import app