"""
Centralized configuration for the Factory Orchestration & Monitoring stack.

All runtime configuration is read from environment variables (loaded from a
local ``.env`` file when present via python-dotenv). This is the single source
of truth for connection settings and credentials — nothing is hardcoded in the
application or in committed config. See ``.env.example`` for the full list.

Defaults target the docker-compose network (service hostnames ``kafka`` /
``influxdb``). To run a service directly on the host, override the relevant
variables, e.g. ``KAFKA_BOOTSTRAP=localhost:9092``.
"""
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # python-dotenv is optional at runtime; env may be set externally
    pass


# ---- Kafka ----
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")

# ---- InfluxDB ----
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "my-super-secret-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "mon-usine")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "donnees-usine")

# ---- Data lake (Parquet archival root) ----
DATA_LAKE_ROOT = os.getenv("DATA_LAKE_ROOT", "/data-lake")
