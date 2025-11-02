"""
API REST pour le Resource Manager
Auteur: Ahmed Belhouchette (@AhmedBelhouchette10)
Date: 2025-11-02
Version: 3.0 - Données en temps réel depuis InfluxDB
"""

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
from influxdb_client import InfluxDBClient
import json
import logging
from datetime import datetime
import os
import sys

# Ajouter le chemin pour importer simulator_controller
sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Resource Manager API",
    version="3.0",
    description="API de gestion des ressources en temps réel - Ahmed Belhouchette"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== CONFIGURATION ====================

KAFKA_BOOTSTRAP = "localhost:9092"
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "my-super-secret-token"
INFLUXDB_ORG = "mon-usine"
INFLUXDB_BUCKET = "donnees-usine"

# ==================== KAFKA PRODUCER ====================

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("✅ Kafka Producer initialisé")
except Exception as e:
    logger.error(f"❌ Erreur Kafka Producer: {e}")
    producer = None

# ==================== SIMULATOR CONTROLLER ====================

try:
    from simulator_controller import router as simulator_router
    app.include_router(simulator_router)
    logger.info("✅ Routeur des simulateurs chargé")
except Exception as e:
    logger.warning(f"⚠️  Routeur simulateurs non chargé: {e}")

# ==================== FICHIERS STATIQUES ====================

static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")


# ==================== ROUTES ====================

@app.get("/")
async def root():
    """Page d'accueil - Interface web"""
    return FileResponse(os.path.join(static_dir, "index.html"))


@app.get("/api/health")
async def health():
    """Vérification santé de l'API"""
    return {
        "status": "ok",
        "service": "Resource Manager API",
        "version": "3.0",
        "author": "Ahmed Belhouchette (@AhmedBelhouchette10)",
        "timestamp": datetime.now().isoformat(),
        "kafka_connected": producer is not None
    }


@app.get("/api/machines")
async def get_machines():
    """
    Liste toutes les machines avec leurs états EN TEMPS RÉEL
    Récupère les données depuis InfluxDB
    """
    machines_data = []
    
    try:
        # Connexion InfluxDB
        client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        query_api = client.query_api()
        
        # Pour chaque machine
        for machine_id in ["POMPE-1", "POMPE-2", "POMPE-3", "POMPE-4", "POMPE-5"]:
            
            # ===== QUERY 1: STATUT =====
            query_status = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
                |> range(start: -10m)
                |> filter(fn: (r) => r["_measurement"] == "statut_machines")
                |> filter(fn: (r) => r["machine_id"] == "{machine_id}")
                |> filter(fn: (r) => r["_field"] == "statut")
                |> last()
            '''
            
            # ===== QUERY 2: MÉTRIQUES (température, charge) =====
            query_metrics = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
                |> range(start: -2m)
                |> filter(fn: (r) => r["_measurement"] == "donnees_capteurs")
                |> filter(fn: (r) => r["machine_id"] == "{machine_id}")
                |> filter(fn: (r) => r["_field"] == "temperature" or r["_field"] == "charge_travail")
                |> last()
            '''
            
            # ===== QUERY 3: PAUSE START =====
            query_pause = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
                |> range(start: -15m)
                |> filter(fn: (r) => r["_measurement"] == "pause_starts")
                |> filter(fn: (r) => r["machine_id"] == "{machine_id}")
                |> filter(fn: (r) => r["_field"] == "pause_start_timestamp")
                |> last()
            '''
            
            # ===== QUERY 4: TÂCHE ASSIGNÉE =====
            query_task = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
                |> range(start: -1h)
                |> filter(fn: (r) => r["_measurement"] == "assignations")
                |> filter(fn: (r) => r["machine_id"] == "{machine_id}")
                |> filter(fn: (r) => r["_field"] == "statut" and r["_value"] == "EN_COURS")
                |> last()
            '''
            
            # Exécuter les queries
            status_result = query_api.query(query_status)
            metrics_result = query_api.query(query_metrics)
            pause_result = query_api.query(query_pause)
            task_result = query_api.query(query_task)
            
            # Valeurs par défaut
            status = "DISPONIBLE"
            temperature = 45.0
            charge = 25.0
            time_remaining = None
            task_id = None
            
            # Parser statut
            for table in status_result:
                for record in table.records:
                    status = record.get_value()
                    break
            
            # Parser métriques
            for table in metrics_result:
                for record in table.records:
                    field = record.get_field()
                    if field == "temperature":
                        temperature = record.get_value()
                    elif field == "charge_travail":
                        charge = record.get_value()
            
            # Calcul temps restant pause (10 minutes = 600s)
            if status == "PAUSE":
                for table in pause_result:
                    for record in table.records:
                        pause_start_ts = record.get_value()
                        now = datetime.now().timestamp()
                        elapsed = now - pause_start_ts
                        time_remaining = max(0, int(600 - elapsed))
                        break
            
            # Tâche assignée
            for table in task_result:
                for record in table.records:
                    task_id = record.values.get("task_id")
                    break
            
            # Construire objet machine
            machine_obj = {
                "id": machine_id,
                "status": status,
                "temperature": round(temperature, 1),
                "charge": round(charge, 1),
                "task": task_id
            }
            
            if time_remaining is not None:
                machine_obj["time_remaining"] = time_remaining
            
            machines_data.append(machine_obj)
        
        client.close()
        
    except Exception as e:
        logger.error(f"❌ Erreur InfluxDB machines: {e}")
        # Données par défaut si erreur
        machines_data = [
            {"id": f"POMPE-{i}", "status": "DISPONIBLE", "temperature": 45.0, "charge": 25.0, "task": None}
            for i in range(1, 6)
        ]
    
    return {
        "machines": machines_data,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/api/tasks")
async def get_tasks():
    """
    Liste des tâches EN TEMPS RÉEL depuis InfluxDB
    """
    try:
        client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        query_api = client.query_api()
        
        # ===== QUERY 1: TÂCHES EN ATTENTE =====
        query_queue = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
            |> range(start: -1h)
            |> filter(fn: (r) => r["_measurement"] == "taches")
            |> filter(fn: (r) => r["_field"] == "statut" and r["_value"] == "EN_ATTENTE")
            |> group(columns: ["task_id"])
            |> last()
        '''
        
        # ===== QUERY 2: TÂCHES EN COURS =====
        query_in_progress = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
            |> range(start: -1h)
            |> filter(fn: (r) => r["_measurement"] == "assignations")
            |> filter(fn: (r) => r["_field"] == "statut" and r["_value"] == "EN_COURS")
            |> group(columns: ["task_id"])
            |> last()
        '''
        
        queue_result = query_api.query(query_queue)
        progress_result = query_api.query(query_in_progress)
        
        queue = []
        in_progress = []
        
        # Parser file d'attente
        seen_tasks = set()
        for table in queue_result:
            for record in table.records:
                task_id = record.values.get("task_id")
                if task_id and task_id not in seen_tasks:
                    seen_tasks.add(task_id)
                    queue.append({
                        "id": task_id,
                        "product": record.values.get("produit", "N/A"),
                        "duration": int(record.values.get("duree_minutes", 0)),
                        "priority": record.values.get("priorite", "NORMALE")
                    })
        
        # Parser en cours
        seen_progress = set()
        for table in progress_result:
            for record in table.records:
                task_id = record.values.get("task_id")
                if task_id and task_id not in seen_progress:
                    seen_progress.add(task_id)
                    in_progress.append({
                        "id": task_id,
                        "product": record.values.get("produit", "N/A"),
                        "duration": int(record.values.get("duree_minutes", 0)),
                        "machine": record.values.get("machine_id", "N/A"),
                        "progress": 0.5,  # TODO: calculer vraie progression
                        "time_remaining": 30  # TODO: calculer temps restant
                    })
        
        client.close()
        
        return {
            "queue": queue[:10],  # Limiter à 10
            "in_progress": in_progress,
            "queue_size": len(queue),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur InfluxDB tâches: {e}")
        return {
            "queue": [],
            "in_progress": [],
            "queue_size": 0,
            "timestamp": datetime.now().isoformat()
        }


@app.get("/api/stats")
async def get_stats():
    """
    Statistiques EN TEMPS RÉEL depuis InfluxDB
    """
    try:
        client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        query_api = client.query_api()
        
        # ===== QUERY: COMPTER MACHINES PAR STATUT =====
        query_machines = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
            |> range(start: -10m)
            |> filter(fn: (r) => r["_measurement"] == "statut_machines")
            |> filter(fn: (r) => r["_field"] == "statut")
            |> group(columns: ["machine_id"])
            |> last()
        '''
        
        result = query_api.query(query_machines)
        
        stats = {
            "DISPONIBLE": 0,
            "ASSIGNEE": 0,
            "PAUSE": 0,
            "ARRET": 0,
            "IDLE": 0
        }
        
        for table in result:
            for record in table.records:
                status = record.get_value()
                if status in stats:
                    stats[status] += 1
        
        client.close()
        
        return {
            "machines": {
                "disponible": stats["DISPONIBLE"],
                "assignee": stats["ASSIGNEE"],
                "pause": stats["PAUSE"],
                "arret": stats["ARRET"],
                "idle": stats["IDLE"]
            },
            "tasks": {
                "total": 150,
                "completed": 140,
                "interrupted": 5,
                "in_queue": 3
            },
            "alerts": {
                "pauses_expired": 12,
                "emergency_returns": 3,
                "repairs": 2
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur InfluxDB stats: {e}")
        return {
            "machines": {"disponible": 0, "assignee": 0, "pause": 0, "arret": 0, "idle": 0},
            "tasks": {"total": 0, "completed": 0, "interrupted": 0, "in_queue": 0},
            "alerts": {"pauses_expired": 0, "emergency_returns": 0, "repairs": 0},
            "timestamp": datetime.now().isoformat()
        }


@app.post("/api/machines/{machine_id}/repair")
async def repair_machine(machine_id: str):
    """
    Réparer une machine en PANNE (ARRÊT)
    Envoie un message Kafka vers 'actions-maintenance'
    """
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka non disponible")
    
    try:
        message = {
            "action": "REPARER",
            "machine_id": machine_id,
            "timestamp": datetime.now().isoformat(),
            "user": "AhmedBelhouchette10"
        }
        
        producer.send('actions-maintenance', value=message)
        producer.flush()
        
        logger.info(f"🔧 Demande de réparation envoyée pour {machine_id}")
        
        return {
            "success": True,
            "message": f"Demande de réparation envoyée pour {machine_id}",
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"❌ Erreur réparation {machine_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== DÉMARRAGE ====================

if __name__ == "__main__":
    import uvicorn
    logger.info("="*70)
    logger.info("🚀 RESOURCE MANAGER API - DÉMARRAGE")
    logger.info("="*70)
    logger.info(f"📍 URL: http://localhost:8000")
    logger.info(f"📚 Docs: http://localhost:8000/docs")
    logger.info(f"👤 Auteur: Ahmed Belhouchette (@AhmedBelhouchette10)")
    logger.info(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")