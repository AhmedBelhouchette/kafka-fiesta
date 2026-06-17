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


@app.middleware("http")
async def no_cache_for_ui(request, call_next):
    """Serve the dashboard HTML/CSS/JS without caching so UI updates always show."""
    response = await call_next(request)
    path = request.url.path
    if path == "/" or path.startswith("/static"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

# ==================== CONFIGURATION ====================

# From environment (see .env.example). Defaults target the docker-compose
# network; override (e.g. KAFKA_BOOTSTRAP=localhost:9092) for host runs.
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "my-super-secret-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "mon-usine")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "donnees-usine")

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
            # etat_machines is the live measurement written by the Spark
            # streaming job and the simulators (donnees_capteurs never existed).
            query_metrics = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
                |> range(start: -5m)
                |> filter(fn: (r) => r["_measurement"] == "etat_machines")
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
            
            # ===== QUERY 4: TÂCHE ASSIGNÉE (+ durée pour calculer la progression) =====
            # group by machine_id+_field so last() collapses across task_ids and
            # returns the SINGLE most recent assignment (not the last per task).
            query_task = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
                |> range(start: -2h)
                |> filter(fn: (r) => r["_measurement"] == "assignations")
                |> filter(fn: (r) => r["machine_id"] == "{machine_id}")
                |> filter(fn: (r) => r["_field"] == "duration_minutes" or r["_field"] == "product_name")
                |> group(columns: ["machine_id", "_field"])
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
            
            # Calcul temps restant pause (env PAUSE_TIMEOUT_SECONDS)
            pause_timeout = int(os.getenv("PAUSE_TIMEOUT_SECONDS", "600"))
            if status == "PAUSE":
                for table in pause_result:
                    for record in table.records:
                        pause_start_ts = record.get_value()
                        elapsed = datetime.now().timestamp() - pause_start_ts
                        time_remaining = max(0, int(pause_timeout - elapsed))
                        break

            # Tâche assignée + progression (depuis la dernière assignation)
            product = None
            task_duration_s = None
            task_started_ts = None
            for table in task_result:
                for record in table.records:
                    task_id = record.values.get("task_id") or task_id
                    if record.get_field() == "product_name":
                        product = record.get_value()
                    elif record.get_field() == "duration_minutes":
                        task_duration_s = float(record.get_value() or 0) * 60
                        rt = record.get_time()
                        if rt:
                            task_started_ts = rt.timestamp()

            progress = None
            task_time_remaining = None
            if status == "ASSIGNEE" and task_duration_s and task_started_ts:
                elapsed = datetime.now().timestamp() - task_started_ts
                progress = max(0.0, min(1.0, elapsed / task_duration_s))
                task_time_remaining = max(0, int(task_duration_s - elapsed))
            else:
                # Not actively running a task: don't surface a stale assignment
                task_id = None
                product = None

            # Construire objet machine
            machine_obj = {
                "id": machine_id,
                "status": status,
                "temperature": round(temperature, 1),
                "charge": round(charge, 1),
                "task": task_id,
                "product": product,
                "progress": round(progress, 3) if progress is not None else None,
                "task_started": task_started_ts if status == "ASSIGNEE" else None,
                "task_duration": int(task_duration_s) if (task_duration_s and status == "ASSIGNEE") else None,
                "task_time_remaining": task_time_remaining,
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
        
        # ===== EN COURS: dernière assignation par machine (mesure 'assignations') =====
        query_assign = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
            |> range(start: -5m)
            |> filter(fn: (r) => r["_measurement"] == "assignations")
            |> filter(fn: (r) => r["_field"] == "product_name" or r["_field"] == "duration_minutes")
            |> group(columns: ["machine_id", "task_id", "_field"])
            |> last()
        '''

        # ===== FILE D'ATTENTE: taille réelle depuis resource_manager_stats =====
        query_queue_size = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
            |> range(start: -10m)
            |> filter(fn: (r) => r["_measurement"] == "resource_manager_stats")
            |> filter(fn: (r) => r["_field"] == "in_queue")
            |> last()
        '''

        tasks_by_id = {}            # task_id -> {id, machine, product, duration}
        latest_per_machine = {}     # machine_id -> (time, task_id)
        for table in query_api.query(query_assign):
            for record in table.records:
                task_id = record.values.get("task_id")
                machine_id = record.values.get("machine_id")
                if not task_id:
                    continue
                entry = tasks_by_id.setdefault(task_id, {
                    "id": task_id, "machine": machine_id,
                    "product": "N/A", "duration": 0,
                })
                if record.get_field() == "product_name":
                    entry["product"] = record.get_value()
                elif record.get_field() == "duration_minutes":
                    entry["duration"] = int(record.get_value() or 0)
                # Track the most recent task per machine
                t = record.get_time()
                if t and (machine_id not in latest_per_machine or t > latest_per_machine[machine_id][0]):
                    latest_per_machine[machine_id] = (t, task_id)

        # One current task per busy machine (a task may have moved machines after
        # a failure/reassignment, so trust the machine key, not first-seen).
        in_progress_list = []
        for machine_id, (_, task_id) in latest_per_machine.items():
            info = tasks_by_id.get(task_id, {})
            in_progress_list.append({
                "id": task_id,
                "machine": machine_id,
                "product": info.get("product", "N/A"),
                "duration": info.get("duration", 0),
            })

        queue_size = 0
        for table in query_api.query(query_queue_size):
            for record in table.records:
                queue_size = int(record.get_value() or 0)

        client.close()

        return {
            "queue": [],  # individual queued tasks live in the orchestrator's memory, not InfluxDB
            "in_progress": in_progress_list,
            "queue_size": queue_size,
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
        
        # ===== QUERY: STATS RÉELLES DU RESOURCE MANAGER =====
        # Real counters written by the Resource Manager each batch.
        query_rms = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
            |> range(start: -10m)
            |> filter(fn: (r) => r["_measurement"] == "resource_manager_stats")
            |> last()
        '''
        rms = {}
        for table in query_api.query(query_rms):
            for record in table.records:
                rms[record.get_field()] = record.get_value()

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
                "total": int(rms.get("total", 0)),
                "completed": int(rms.get("completed", 0)),
                "interrupted": int(rms.get("interrupted", 0)),
                "in_queue": int(rms.get("in_queue", 0))
            },
            "alerts": {
                "pauses_expired": int(rms.get("pauses_expired", 0)),
                "emergency_returns": int(rms.get("emergency_returns", 0)),
                "repairs": int(rms.get("repairs", 0))
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