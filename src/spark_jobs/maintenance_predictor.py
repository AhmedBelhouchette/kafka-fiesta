"""
Maintenance predictor (Spark Structured Streaming + scikit-learn).

Consumes ``donnees-capteurs``, scores each reading with the trained
RandomForest model, and writes the failure probability to the InfluxDB
``predictions`` measurement (``probabilite_panne``) defined in SPECIFICATIONS.md.

This is additive to the lower-latency Kafka alert path in
``ml_predictor_fixed.py`` (which raises actionable alerts on ``alertes-ml``):
here Spark is used to populate the time-series ``predictions`` layer that
Grafana visualizes. Inference runs driver-side per micro-batch.
"""
import os
import pickle
import logging

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("maintenance_predictor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
INPUT_TOPIC = os.getenv("SENSOR_TOPIC", "donnees-capteurs")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
KAFKA_SQL_PACKAGE = os.getenv(
    "KAFKA_SQL_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)
MODEL_DIR = os.getenv("MODEL_DIR", "/app/models")

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "my-super-secret-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "mon-usine")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "donnees-usine")

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

_model = None
_scaler = None


def load_model():
    global _model, _scaler
    model_path = os.path.join(MODEL_DIR, "rf_model.pkl")
    scaler_path = os.path.join(MODEL_DIR, "scaler.pkl")
    if os.path.exists(model_path) and os.path.exists(scaler_path):
        with open(model_path, "rb") as f:
            _model = pickle.load(f)
        with open(scaler_path, "rb") as f:
            _scaler = pickle.load(f)
        logger.info("Loaded RandomForest model from %s", MODEL_DIR)
    else:
        logger.warning("Model not found in %s — falling back to a rule-based score", MODEL_DIR)


def feature_vector(r):
    """Single-reading features in the predictor's 12-feature order (no window:
    rolling means = current value, std/slope = 0). Matches scripts/train_model.py."""
    vib = r.get("vibration") or 0.0
    temp = r.get("temperature") or 0.0
    pres = r.get("pression") or 0.0
    conso = r.get("consommation_electrique") or 0.0
    charge = r.get("charge_travail") or 0.0
    return [vib, temp, pres, conso, charge, vib, temp, 0.0, 0.0, 0.0, 0.0, temp]


def failure_probability(r):
    if _model is not None and _scaler is not None:
        x = _scaler.transform(np.array([feature_vector(r)]))
        proba = _model.predict_proba(x)[0]
        # P(needs action) = 1 - P(class 0 'aucune')
        classes = list(_model.classes_)
        p_none = proba[classes.index(0)] if 0 in classes else 0.0
        return float(1.0 - p_none)
    # Rule-based fallback
    temp = r.get("temperature") or 0.0
    charge = r.get("charge_travail") or 0.0
    if temp > 90 or charge > 90:
        return 0.95
    if temp > 75 or charge > 70:
        return 0.6
    return 0.05


def process_batch(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return
    from influxdb_client import InfluxDBClient, Point
    from influxdb_client.client.write_api import SYNCHRONOUS

    points = []
    for row in batch_df.collect():
        r = row.asDict()
        prob = failure_probability(r)
        points.append(
            Point("predictions")
            .tag("machine_id", r["machine_id"])
            .field("probabilite_panne", round(prob, 4))
        )
    if points:
        with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client:
            client.write_api(write_options=SYNCHRONOUS).write(bucket=INFLUXDB_BUCKET, record=points)
    logger.info("Batch %s: wrote %d failure predictions", epoch_id, len(points))


def main():
    load_model()
    logger.info("Starting maintenance predictor | master=%s | kafka=%s", SPARK_MASTER, KAFKA_BOOTSTRAP)
    spark = (
        SparkSession.builder
        .appName("maintenance-predictor")
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
        .option("checkpointLocation", os.getenv("DATA_LAKE_ROOT", "/data-lake") + "/_checkpoints/predictions")
        .trigger(processingTime="20 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
