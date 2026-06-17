"""
Energy anomaly monitor (Spark Structured Streaming).

Consumes ``donnees-capteurs`` and flags two anomaly types per SPECIFICATIONS.md:
  * ``surconsommation`` — power draw above the high threshold;
  * ``fuite`` — high power draw while workload is low (energy "leak").

Each anomaly is published to the ``alertes-energie`` Kafka topic and written to
the InfluxDB ``anomalies_energie`` measurement. Self-contained; defaults to
local Spark (override with SPARK_MASTER).
"""
import os
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("energy_monitor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
INPUT_TOPIC = os.getenv("SENSOR_TOPIC", "donnees-capteurs")
OUTPUT_TOPIC = os.getenv("ENERGY_TOPIC", "alertes-energie")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
KAFKA_SQL_PACKAGE = os.getenv(
    "KAFKA_SQL_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "my-super-secret-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "mon-usine")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "donnees-usine")

# Thresholds (consommation_electrique is simulated in the 80-150 range)
VALEUR_NORMALE = float(os.getenv("ENERGY_NORMAL", "110"))
SEUIL_SURCONSO = float(os.getenv("ENERGY_HIGH", "135"))
SEUIL_FUITE_CONSO = float(os.getenv("ENERGY_LEAK_CONSO", "120"))
SEUIL_FUITE_CHARGE = float(os.getenv("ENERGY_LEAK_CHARGE", "30"))

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


def classify(conso, charge):
    """Return an anomaly type or None."""
    if conso is None:
        return None
    if conso > SEUIL_SURCONSO:
        return "surconsommation"
    if conso > SEUIL_FUITE_CONSO and (charge is not None and charge < SEUIL_FUITE_CHARGE):
        return "fuite"
    return None


def process_batch(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return
    from kafka import KafkaProducer
    from influxdb_client import InfluxDBClient, Point
    from influxdb_client.client.write_api import SYNCHRONOUS

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    points = []
    alerts = 0
    for row in batch_df.collect():
        r = row.asDict()
        type_alerte = classify(r.get("consommation_electrique"), r.get("charge_travail"))
        if not type_alerte:
            continue
        now = datetime.now(timezone.utc).isoformat()
        alert = {
            "machine_id": r["machine_id"],
            "type_alerte": type_alerte,
            "valeur_actuelle": round(float(r["consommation_electrique"]), 2),
            "valeur_normale": VALEUR_NORMALE,
            "timestamp_detection": now,
        }
        producer.send(OUTPUT_TOPIC, value=alert)
        points.append(
            Point("anomalies_energie")
            .tag("machine_id", r["machine_id"])
            .tag("type_alerte", type_alerte)
            .field("valeur_actuelle", alert["valeur_actuelle"])
            .field("valeur_normale", VALEUR_NORMALE)
        )
        alerts += 1
    producer.flush()
    producer.close()

    if points:
        with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client:
            client.write_api(write_options=SYNCHRONOUS).write(bucket=INFLUXDB_BUCKET, record=points)
    logger.info("Batch %s: %d energy anomalies", epoch_id, alerts)


def main():
    logger.info("Starting energy monitor | master=%s | kafka=%s", SPARK_MASTER, KAFKA_BOOTSTRAP)
    spark = (
        SparkSession.builder
        .appName("energy-monitor")
        .master(SPARK_MASTER)
        .config("spark.jars.packages", KAFKA_SQL_PACKAGE)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    parsed = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("d"))
        .select("d.*")
        .filter(col("machine_id").isNotNull())
    )

    query = (
        parsed.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", os.getenv("DATA_LAKE_ROOT", "/data-lake") + "/_checkpoints/energy")
        .trigger(processingTime="20 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
