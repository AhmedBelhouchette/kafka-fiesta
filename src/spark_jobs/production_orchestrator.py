import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, rank, desc, lit
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StringType, FloatType, LongType, TimestampType
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
CAPTEURS_TOPIC = "donnees-capteurs"
COMMANDES_TOPIC = "commandes-machines"

INFLUXDB_URL = "http://influxdb:8086"
INFLUXDB_TOKEN = "my-super-secret-token"
INFLUXDB_ORG = "mon-usine"
INFLUXDB_BUCKET = "donnees-usine"

# --- Schéma des données Kafka ---
schema_capteurs = StructType() \
    .add("machine_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("etat", StringType()) \
    .add("charge_travail", FloatType()) \
    .add("consommation_electrique", FloatType()) \
    .add("vibration", FloatType()) \
    .add("temperature", FloatType()) \
    .add("pression", FloatType())

# --- Initialisation de l'API InfluxDB ---
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# --- Fonction de traitement pour chaque micro-batch ---
def process_batch(batch_df, epoch_id):
    print(f"\n--- Traitement du Batch ID: {epoch_id} ---")

    if batch_df.rdd.isEmpty():
        print("Batch vide, rien à traiter.")
        return

    windowSpec = Window.partitionBy("machine_id").orderBy(desc("timestamp"))
    latest_machine_states = batch_df.withColumn("rank", rank().over(windowSpec)) \
                                    .filter(col("rank") == 1) \
                                    .select("machine_id", "etat", "charge_travail", "timestamp", "temperature", "vibration", "consommation_electrique")
    
    latest_machine_states.persist()
    
    print("État le plus récent des machines dans ce batch :")
    latest_machine_states.show(5, truncate=False)

    points = latest_machine_states.rdd.map(
        lambda row: Point("etat_machines")
                    .tag("machine_id", row.machine_id)
                    .field("etat", row.etat)
                    .field("charge_travail", float(row.charge_travail))
                    .field("temperature", float(row.temperature))
                    .field("vibration", float(row.vibration))
                    .field("consommation_electrique", float(row.consommation_electrique))
                    .time(row.timestamp)
    ).collect()

    if points:
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
        print(f"==> {len(points)} points d'état écrits dans InfluxDB.")

    latest_machine_states.unpersist()


# --- Main Spark Application ---
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("ProductionOrchestrator") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df_capteurs = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", CAPTEURS_TOPIC) \
        .load()

    df_parsed = df_capteurs.select(from_json(col("value").cast("string"), schema_capteurs).alias("data")).select("data.*")
    
    query = df_parsed.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime='15 seconds') \
        .start()

    query.awaitTermination()