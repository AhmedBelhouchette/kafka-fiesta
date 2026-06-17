"""
SparkSession factory helpers.

``get_spark_session`` keeps the original minimal signature; ``get_streaming_session``
returns a session pre-configured with the Kafka SQL connector and the standalone
master (overridable via the SPARK_MASTER env var) for the streaming jobs.
"""
import os

from pyspark.sql import SparkSession

KAFKA_SQL_PACKAGE = os.getenv(
    "KAFKA_SQL_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)


def get_spark_session(app_name: str = "kafka_fiesta_app"):
    """Return a basic SparkSession (master taken from SPARK_MASTER if set)."""
    builder = SparkSession.builder.appName(app_name)
    master = os.getenv("SPARK_MASTER")
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def get_streaming_session(app_name: str):
    """SparkSession wired for Kafka structured streaming."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", KAFKA_SQL_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
