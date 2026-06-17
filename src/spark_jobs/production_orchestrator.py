"""
Production orchestrator entrypoint.

Launches the Spark Structured Streaming archival pipeline (Kafka -> Parquet data
lake + InfluxDB hot layer). Kept as a thin entrypoint so it runs the same way
whether started directly (`python production_orchestrator.py`, local Spark) or
pointed at the standalone cluster via SPARK_MASTER=spark://spark-master:7077.
"""
import os
import sys

# Make the sibling `streaming` package importable when run as a script.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from streaming.archival_job import main  # noqa: E402

if __name__ == "__main__":
    main()
