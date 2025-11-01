"""
Resource Manager - Assignation Intelligente des Tâches de Production
Lit: plan-de-production, alertes-ml, etat_machines (InfluxDB)
Écrit: assignations-taches (Kafka), statut_machines (InfluxDB)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, lit, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import json
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import sys
import os

# Ajouter le chemin utils au PYTHONPATH
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.machine_selector import MachineSelector


# ========================
# CONFIGURATION
# ========================

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
INFLUXDB_URL = "http://influxdb:8086"
INFLUXDB_TOKEN = "my-super-secret-token"
INFLUXDB_ORG = "mon-usine"
INFLUXDB_BUCKET = "donnees-usine"

# IDs des machines (ajustez selon votre config)
MACHINES_IDS = ["POMPE-1", "POMPE-2", "POMPE-3", "POMPE-4", "POMPE-5"]

# État global des machines occupées (sera mis à jour en mémoire)
machines_occupees = {}  # {machine_id: datetime_fin_estimee}
file_attente = []  # Liste des tâches en attente


# ========================
# SCHÉMAS KAFKA
# ========================

# Schéma pour le plan de production
schema_plan_production = StructType([
    StructField("tache_id", StringType(), True),
    StructField("type_produit", StringType(), True),
    StructField("quantite", IntegerType(), True)
])

# Schéma pour les alertes ML
schema_alertes_ml = StructType([
    StructField("alert_id", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("timestamp_detection", StringType(), True),
    StructField("type_panne", StringType(), True),
    StructField("probabilite_panne", FloatType(), True),
    StructField("action_recommandee", StringType(), True)
])


# ========================
# CONNEXION INFLUXDB
# ========================

influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
influx_write_api = influx_client.write_api(write_options=SYNCHRONOUS)
influx_query_api = influx_client.query_api()


def lire_etat_machines():
    """
    Lit l'état actuel de toutes les machines depuis InfluxDB
    
    Returns:
        Dict[machine_id, {temperature, vibration, charge_travail, etat, etc.}]
    """
    query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -5m)
      |> filter(fn: (r) => r._measurement == "etat_machines")
      |> last()
      |> pivot(rowKey:["machine_id"], columnKey: ["_field"], valueColumn: "_value")
    '''
    
    try:
        result = influx_query_api.query(query)
        etats = {}
        
        for table in result:
            for record in table.records:
                machine_id = record.values.get('machine_id')
                if machine_id:
                    etats[machine_id] = {
                        'temperature': record.values.get('temperature', 0),
                        'vibration': record.values.get('vibration', 0),
                        'charge_travail': record.values.get('charge_travail', 0),
                        'consommation_electrique': record.values.get('consommation_electrique', 0),
                        'etat': record.values.get('etat', 'inconnu'),
                        'nombre_taches': record.values.get('nombre_taches', 0)
                    }
                    print(f"✅ État récupéré pour {machine_id}: temp={etats[machine_id]['temperature']}°C, charge={etats[machine_id]['charge_travail']}%")
        
        if not etats:
            print("⚠️ Aucune donnée machine trouvée dans InfluxDB (etat_machines)")
        
        return etats
        
    except Exception as e:
        print(f"❌ Erreur lecture InfluxDB: {e}")
        import traceback
        traceback.print_exc()
        return {}


def ecrire_statut_machine(machine_id, tache_id, quantite, duree_minutes, statut="ASSIGNEE"):
    """
    Écrit le statut d'une machine dans InfluxDB pour Grafana
    """
    maintenant = datetime.utcnow()
    fin_estimee = maintenant + timedelta(minutes=duree_minutes)
    
    point = Point("statut_machines") \
        .tag("machine_id", machine_id) \
        .field("tache_en_cours", tache_id) \
        .field("quantite_assignee", quantite) \
        .field("duree_minutes", duree_minutes) \
        .field("temps_restant_minutes", duree_minutes) \
        .field("statut", statut) \
        .field("fin_estimee", fin_estimee.isoformat()) \
        .time(maintenant)
    
    try:
        influx_write_api.write(bucket=INFLUXDB_BUCKET, record=point)
        print(f"   📊 Statut écrit dans InfluxDB: {machine_id} -> {statut}")
    except Exception as e:
        print(f"❌ Erreur écriture InfluxDB statut: {e}")


def ecrire_assignation_historique(tache_id, machine_id, quantite, duree_minutes):
    """
    Écrit l'historique des assignations dans InfluxDB
    """
    point = Point("assignations_historique") \
        .tag("machine_id", machine_id) \
        .tag("tache_id", tache_id) \
        .field("quantite", quantite) \
        .field("duree_minutes", duree_minutes) \
        .field("statut", "EN_COURS") \
        .time(datetime.utcnow())
    
    try:
        influx_write_api.write(bucket=INFLUXDB_BUCKET, record=point)
    except Exception as e:
        print(f"❌ Erreur écriture historique: {e}")


def ecrire_file_attente(nombre_taches):
    """
    Écrit le nombre de tâches en file d'attente dans InfluxDB
    """
    point = Point("file_attente") \
        .field("nombre_taches", nombre_taches) \
        .time(datetime.utcnow())
    
    try:
        influx_write_api.write(bucket=INFLUXDB_BUCKET, record=point)
    except Exception as e:
        print(f"❌ Erreur écriture file attente: {e}")


def traiter_assignation(tache, alertes_ml_dict):
    """
    Traite une tâche et l'assigne à une machine si possible
    
    Args:
        tache: Dict avec tache_id, type_produit, quantite
        alertes_ml_dict: Dict des alertes ML par machine_id
        
    Returns:
        Dict de l'assignation ou None si pas de machine disponible
    """
    global machines_occupees
    
    tache_id = tache['tache_id']
    quantite = tache['quantite']
    
    print(f"\n{'='*60}")
    print(f"📋 Traitement de la tâche: {tache_id}")
    print(f"   Produit: {tache['type_produit']}, Quantité: {quantite}")
    
    # Nettoyer les machines qui ont fini leur tâche
    maintenant = datetime.utcnow()
    machines_liberees = [mid for mid, fin in machines_occupees.items() if fin <= maintenant]
    for mid in machines_liberees:
        del machines_occupees[mid]
        print(f"   ✅ Machine {mid} libérée")
    
    # Lire l'état actuel des machines depuis InfluxDB
    etats_machines = lire_etat_machines()
    
    if not etats_machines:
        print("   ⚠️ Impossible de lire l'état des machines depuis InfluxDB")
        print("   📥 Tâche mise en file d'attente")
        return None
    
    # Sélectionner la meilleure machine
    result = MachineSelector.selectionner_meilleure_machine(
        MACHINES_IDS,
        etats_machines,
        alertes_ml_dict,
        machines_occupees
    )
    
    if result is None or result[0] is None:
        raison = result[1] if result else "Erreur inconnue"
        print(f"   ❌ Aucune machine disponible: {raison}")
        print(f"   📥 Tâche mise en file d'attente")
        return None
    
    machine_id, raison = result
    duree_minutes = MachineSelector.calculer_duree_tache(quantite)
    fin_estimee = maintenant + timedelta(minutes=duree_minutes)
    
    # Marquer la machine comme occupée
    machines_occupees[machine_id] = fin_estimee
    
    print(f"   ✅ ASSIGNATION RÉUSSIE")
    print(f"   Machine: {machine_id}")
    print(f"   Raison: {raison}")
    print(f"   Durée estimée: {duree_minutes} minutes")
    print(f"   Fin estimée: {fin_estimee.strftime('%H:%M:%S')}")
    
    # Écrire dans InfluxDB
    ecrire_statut_machine(machine_id, tache_id, quantite, duree_minutes)
    ecrire_assignation_historique(tache_id, machine_id, quantite, duree_minutes)
    
    # Créer le message d'assignation
    assignation = {
        "tache_id": tache_id,
        "machine_id": machine_id,
        "quantite_assignee": quantite,
        "timestamp_assignation": maintenant.isoformat() + "Z",
        "duree_estimee_minutes": duree_minutes,
        "fin_estimee": fin_estimee.isoformat() + "Z",
        "statut": "ASSIGNEE"
    }
    
    return assignation


def traiter_batch(batch_df, batch_id, alertes_ml_dict):
    """
    Traite un batch de tâches du plan de production
    """
    print(f"\n{'#'*60}")
    print(f"# BATCH {batch_id} - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")
    
    if batch_df.isEmpty():
        print("Aucune tâche dans ce batch")
        return
    
    taches = batch_df.collect()
    print(f"📦 {len(taches)} tâche(s) à traiter")
    
    assignations = []
    
    for row in taches:
        tache = row.asDict()
        assignation = traiter_assignation(tache, alertes_ml_dict)
        
        if assignation:
            assignations.append(assignation)
        else:
            file_attente.append(tache)
    
    # Écrire la taille de la file d'attente
    ecrire_file_attente(len(file_attente))
    
    print(f"\n📊 RÉSUMÉ DU BATCH:")
    print(f"   ✅ Assignations réussies: {len(assignations)}")
    print(f"   📥 Tâches en attente: {len(file_attente)}")
    print(f"   🔧 Machines occupées: {len(machines_occupees)}")
    
    # Publier les assignations dans Kafka
    if assignations:
        producer_df = spark.createDataFrame(assignations)
        producer_df.select(to_json(struct([col(c) for c in producer_df.columns])).alias("value")) \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", "assignations-taches") \
            .save()
        
        print(f"   ✉️ {len(assignations)} assignation(s) publiée(s) dans Kafka")


# ========================
# SPARK SESSION
# ========================

print("🚀 Démarrage du Resource Manager...")

spark = SparkSession.builder \
    .appName("ResourceManager") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark Session créée")


# ========================
# LECTURE DES STREAMS
# ========================

# Stream 1: Plan de production
print("📖 Lecture du topic 'plan-de-production'...")
stream_plan_production = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "plan-de-production") \
    .option("startingOffsets", "latest") \
    .load()

taches_df = stream_plan_production.select(
    from_json(col("value").cast("string"), schema_plan_production).alias("data")
).select("data.*")

# Stream 2: Alertes ML (on les stocke en mémoire pour consultation)
print("📖 Lecture du topic 'alertes-ml'...")
alertes_ml_dict = {}  # {machine_id: alerte_dict}

def update_alertes_ml(batch_df, batch_id):
    """Met à jour le dictionnaire des alertes ML"""
    global alertes_ml_dict
    
    if batch_df.isEmpty():
        return
    
    for row in batch_df.collect():
        alerte = row.asDict()
        machine_id = alerte.get('machine_id')
        if machine_id:
            alertes_ml_dict[machine_id] = alerte
            action = alerte.get('action_recommandee', 'INCONNU')
            emoji = "🟢" if action == "CONTINUER" else "🟡" if action == "PAUSE" else "🔴"
            print(f"{emoji} Alerte ML mise à jour pour {machine_id}: {action}")

stream_alertes_ml = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "alertes-ml") \
    .option("startingOffsets", "latest") \
    .load()

alertes_df = stream_alertes_ml.select(
    from_json(col("value").cast("string"), schema_alertes_ml).alias("data")
).select("data.*")

# Lancer le stream des alertes ML en arrière-plan
query_alertes = alertes_df.writeStream \
    .foreachBatch(update_alertes_ml) \
    .outputMode("append") \
    .start()


# ========================
# TRAITEMENT PRINCIPAL
# ========================

print("🎯 Lancement du traitement des tâches...")

query_taches = taches_df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: traiter_batch(batch_df, batch_id, alertes_ml_dict)) \
    .outputMode("append") \
    .trigger(processingTime='15 seconds') \
    .start()

print("✅ Resource Manager opérationnel !")
print("📊 En attente de tâches depuis Kafka...")

# Attendre les streams
query_taches.awaitTermination()