"""
Spark Structured Streaming — raw archival + hot serving.

Reads the ``donnees-capteurs`` Kafka topic and, per micro-batch:
  1. appends every raw reading to a Parquet **data lake**, partitioned
     ``year=/month=/day=`` (the HDFS-compatible layout from SPECIFICATIONS.md);
  2. writes the readings to the InfluxDB ``etat_machines`` measurement (the hot
     serving layer Grafana / the API read from).

Self-contained on purpose: it reads all configuration from environment
variables and builds its own SparkSession, so it runs identically via
``python archival_job.py`` (local Spark) or against the standalone cluster
(set ``SPARK_MASTER=spark://spark-master:7077``).
"""
import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, year, month, dayofmonth,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("archival_job")

# ---- Configuration (env) ----
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.getenv("SENSOR_TOPIC", "donnees-capteurs")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
KAFKA_SQL_PACKAGE = os.getenv(
    "KAFKA_SQL_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)
DATA_LAKE_ROOT = os.getenv("DATA_LAKE_ROOT", "/data-lake")
LAKE_PATH = f"{DATA_LAKE_ROOT}/raw/donnees-capteurs"
CHECKPOINT_PATH = f"{DATA_LAKE_ROOT}/_checkpoints/archival"

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "my-super-secret-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "mon-usine")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "donnees-usine")

# Schema of donnees-capteurs (see SPECIFICATIONS.md)
SENSOR_SCHEMA = StructType([
    StructField("machine_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("etat", StringType()),
    StructField("charge_travail", DoubleType()),
    StructField("consommation_electrique", DoubleType()),
    StructField("vibration", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("pression", DoubleType()),
])


def build_spark():
    return (
        SparkSession.builder
        .appName("factory-archival")
        .master(SPARK_MASTER)
        .config("spark.jars.packages", KAFKA_SQL_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def write_to_influx(rows):
    """Write a batch of sensor rows to the InfluxDB etat_machines measurement.

    Imported lazily and called on the driver (after collect) so executors don't
    need the influxdb client.
    """
    if not rows:
        return
    from influxdb_client import InfluxDBClient, Point
    from influxdb_client.client.write_api import SYNCHRONOUS

    with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        points = []
        for r in rows:
            p = (
                Point("etat_machines")
                .tag("machine_id", r["machine_id"])
                .field("etat", r["etat"] if r["etat"] is not None else "")
                .field("charge_travail", float(r["charge_travail"] or 0.0))
                .field("temperature", float(r["temperature"] or 0.0))
                .field("vibration", float(r["vibration"] or 0.0))
                .field("consommation_electrique", float(r["consommation_electrique"] or 0.0))
            )
            points.append(p)
        write_api.write(bucket=INFLUXDB_BUCKET, record=points)
    logger.info("Wrote %d points to InfluxDB etat_machines", len(points))


def process_batch(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return
    batch_df = batch_df.persist()
    try:
        # 1) Raw archival to the partitioned Parquet data lake
        (
            batch_df.write
            .mode("append")
            .partitionBy("year", "month", "day")
            .parquet(LAKE_PATH)
        )
        # 2) Hot serving layer (driver-side write of the collected batch)
        rows = [row.asDict() for row in batch_df.collect()]
        write_to_influx(rows)
        logger.info("Batch %s: archived %d readings", epoch_id, len(rows))
    finally:
        batch_df.unpersist()


def main():
    logger.info("Starting archival stream | master=%s | kafka=%s | lake=%s",
                SPARK_MASTER, KAFKA_BOOTSTRAP, LAKE_PATH)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withColumn("year", year(col("event_time")))
        .withColumn("month", month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))
        .filter(col("machine_id").isNotNull())
    )

    query = (
        parsed.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="20 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
