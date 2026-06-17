"""
Contrôleur API pour les simulateurs - VERSION DÉCOUPLÉE AVEC KAFKA PERMANENT
Le Producer Kafka reste actif en permanence
On contrôle juste l'envoi des messages avec un flag running
Auteur: Ahmed Belhouchette (@AhmedBelhouchette10)
Date: 2025-11-02
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import threading
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
from influxdb_client import InfluxDBClient
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/simulators", tags=["Simulators"])

# ==================== KAFKA & INFLUXDB PERMANENTS ====================

# Producer Kafka UNIQUE et PERMANENT
kafka_producer = None
influx_client = None
influx_query_api = None

def init_connections():
    """Initialise les connexions Kafka et InfluxDB UNE SEULE FOIS"""
    global kafka_producer, influx_client, influx_query_api
    
    if kafka_producer is None:
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:29092"),
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("✅ Kafka Producer PERMANENT initialisé")
        except Exception as e:
            logger.error(f"❌ Erreur Kafka Producer: {e}")
    
    if influx_client is None:
        try:
            influx_client = InfluxDBClient(
                url=os.getenv("INFLUXDB_URL", "http://influxdb:8086"),
                token=os.getenv("INFLUXDB_TOKEN", "my-super-secret-token"),
                org=os.getenv("INFLUXDB_ORG", "mon-usine")
            )
            influx_query_api = influx_client.query_api()
            logger.info("✅ InfluxDB Client PERMANENT initialisé")
        except Exception as e:
            logger.error(f"❌ Erreur InfluxDB: {e}")

# Initialiser au démarrage du module
init_connections()

# État des simulateurs
simulator_states = {
    "capteurs": {
        "running": False,
        "thread": None,
        "frequency": int(os.getenv("SENSOR_INTERVAL", "5")),
        "error_rate": float(os.getenv("CAPTEURS_ERROR_RATE", "0.08"))
    },
    "production": {
        "running": False,
        "thread": None,
        "frequency": int(os.getenv("PRODUCTION_INTERVAL", "10")),
        "task_counter": 1040
    }
}

# Task durations in minutes (short by default so the lifecycle is watchable).
TASK_MINUTES = [int(x) for x in os.getenv("DEMO_TASK_MINUTES", "1,2,3").split(",") if x.strip()] or [1, 2, 3]

# Modèles Pydantic
class SimulatorConfig(BaseModel):
    frequency: int = None
    error_rate: float = None

class ForceErrorRequest(BaseModel):
    machine_id: str


# ==================== BOUCLES DE SIMULATION ====================

def _machine_status(machine_id):
    """Latest status of a machine from InfluxDB (statut_machines)."""
    if not influx_query_api:
        return "DISPONIBLE"
    try:
        q = (f'from(bucket:"donnees-usine") |> range(start:-15m) '
             f'|> filter(fn:(r)=>r["_measurement"]=="statut_machines") '
             f'|> filter(fn:(r)=>r["machine_id"]=="{machine_id}") '
             f'|> filter(fn:(r)=>r["_field"]=="statut") |> last()')
        for table in influx_query_api.query(q):
            for record in table.records:
                return record.get_value()
    except Exception as e:
        logger.warning(f"⚠️  statut {machine_id}: {e}")
    return "DISPONIBLE"


def capteurs_simulation_loop():
    """Sensor simulation driven by a per-machine DEGRADATION process.

    Each working machine accumulates wear; the sensor readings are the same noisy
    functions of wear used to train the model (scripts/train_model.py), so the
    failure predictor sees real rising trends and flags machines *before* failure.
    A COOLDOWN acts as preventive maintenance (wear drops); a repair resets wear.
    The error-rate slider controls the probability of a degradation 'shock'.
    """
    logger.info("🚀 Thread simulateur capteurs (dégradation) démarré")
    machines = ["POMPE-1", "POMPE-2", "POMPE-3", "POMPE-4", "POMPE-5"]
    wear = {m: random.uniform(0.0, 0.1) for m in machines}
    prev = {m: "DISPONIBLE" for m in machines}

    def clip(x, lo, hi):
        return max(lo, min(hi, x))

    while simulator_states["capteurs"]["running"]:
        shock_rate = simulator_states["capteurs"]["error_rate"]
        for mid in machines:
            status = _machine_status(mid)
            if prev[mid] == "ARRET" and status != "ARRET":
                wear[mid] = 0.0          # repaired -> as-new
            prev[mid] = status

            if status in ("DISPONIBLE", "ASSIGNEE"):
                inc = random.gammavariate(2.0, 0.010)        # noisy wear increment
                if random.random() < shock_rate:
                    inc += random.uniform(0.06, 0.14)        # degradation shock
                wear[mid] = clip(wear[mid] + inc, 0.0, 1.6)
                w = wear[mid]
                load = clip(random.uniform(0.35, 0.85) + 0.1 * random.gauss(0, 1), 0.05, 1.0)
                data = {
                    "machine_id": mid,
                    "timestamp": datetime.now().isoformat(),
                    "temperature": round(58 + 28 * w + 9 * load + random.gauss(0, 4), 2),
                    "vibration": round(0.7 + 2.6 * (w ** 1.6) + (0.4 + 0.8 * w) * abs(random.gauss(0, 1)), 3),
                    "pression": round(5.0 + 1.1 * w + random.gauss(0, 0.4), 2),
                    "consommation_electrique": round(95 + 32 * w + 16 * load + random.gauss(0, 8), 2),
                    "charge_travail": round(clip(100 * load + random.gauss(0, 3), 0, 100), 2),
                }
                if kafka_producer:
                    kafka_producer.send('donnees-capteurs', value=data)
                logger.info(f"  ✅ {mid:8} wear={w:.2f}  T={data['temperature']:5.1f}°C  V={data['vibration']:.2f}")
            elif status == "PAUSE":
                wear[mid] = max(0.0, wear[mid] * 0.6)         # cooldown = light maintenance
                logger.info(f"  ❄️  {mid:8} cooldown -> wear={wear[mid]:.2f}")
            # ARRET: broken, emit nothing

        if kafka_producer:
            kafka_producer.flush()
        time.sleep(simulator_states["capteurs"]["frequency"])

    logger.info("🛑 Thread simulateur capteurs terminé")


def production_simulation_loop():
    """
    Boucle de simulation de production
    Utilise le Producer Kafka PERMANENT
    """
    logger.info("🚀 Thread simulateur production démarré")
    
    products = {
        "Product A": "NORMALE", "Product B": "NORMALE", "Product C": "NORMALE",
        "Product D": "HAUTE", "Product E": "HAUTE",
    }

    while simulator_states["production"]["running"]:
        # Générer tâche
        simulator_states["production"]["task_counter"] += 1
        task_id = f"TASK-{simulator_states['production']['task_counter']}{random.choice(['A','B','C','D','E','F'])}"

        product_name = random.choice(list(products.keys()))
        duration = random.choice(TASK_MINUTES)

        task = {
            "task_id": task_id,
            "product": product_name,
            "duration_minutes": duration,
            "priority": products[product_name],
            "timestamp": datetime.now().isoformat(),
            "status": "EN_ATTENTE"
        }
        
        # Envoyer via Kafka PERMANENT
        if kafka_producer:
            kafka_producer.send('plan-de-production', value=task)
            kafka_producer.flush()
        
        priority_emoji = "🔴" if task["priority"] == "HAUTE" else "🟡"
        logger.info(
            f"📦 Nouvelle tâche: {task_id:12} | "
            f"{product_name:10} | "
            f"{priority_emoji} {task['priority']:8} | "
            f"⏱️  {task['duration_minutes']} min"
        )
        
        time.sleep(simulator_states["production"]["frequency"])
    
    logger.info("🛑 Thread simulateur production terminé")


# ==================== ENDPOINTS API ====================

@router.post("/capteurs/start")
async def start_capteurs_simulator():
    """Démarre le simulateur de capteurs"""
    if simulator_states["capteurs"]["running"]:
        raise HTTPException(status_code=400, detail="Déjà démarré")
    
    if not kafka_producer:
        raise HTTPException(status_code=503, detail="Kafka non disponible")
    
    # Marquer comme running
    simulator_states["capteurs"]["running"] = True
    
    # Lancer dans un thread
    thread = threading.Thread(target=capteurs_simulation_loop, daemon=True)
    thread.start()
    simulator_states["capteurs"]["thread"] = thread
    
    logger.info("▶️  Simulateur de capteurs démarré")
    
    return {
        "success": True,
        "message": "Simulateur de capteurs démarré",
        "config": {
            "frequency": simulator_states["capteurs"]["frequency"],
            "error_rate": simulator_states["capteurs"]["error_rate"]
        }
    }


@router.post("/capteurs/stop")
async def stop_capteurs_simulator():
    """Arrête le simulateur de capteurs (Kafka reste actif)"""
    if not simulator_states["capteurs"]["running"]:
        raise HTTPException(status_code=400, detail="N'est pas démarré")
    
    # Marquer comme stopped
    simulator_states["capteurs"]["running"] = False
    
    logger.info("⏸️  Simulateur de capteurs arrêté (Kafka reste actif)")
    
    return {"success": True, "message": "Simulateur arrêté (Kafka actif)"}


@router.put("/capteurs/config")
async def configure_capteurs_simulator(config: SimulatorConfig):
    """Configure le simulateur de capteurs"""
    if config.frequency is not None:
        simulator_states["capteurs"]["frequency"] = max(1, min(30, config.frequency))
    
    if config.error_rate is not None:
        simulator_states["capteurs"]["error_rate"] = max(0.0, min(1.0, config.error_rate))
    
    return {
        "success": True,
        "message": "Configuration mise à jour",
        "config": {
            "frequency": simulator_states["capteurs"]["frequency"],
            "error_rate": simulator_states["capteurs"]["error_rate"]
        }
    }


@router.get("/capteurs/status")
async def get_capteurs_status():
    """Retourne le statut du simulateur de capteurs"""
    return {
        "running": simulator_states["capteurs"]["running"],
        "frequency": simulator_states["capteurs"]["frequency"],
        "error_rate": simulator_states["capteurs"]["error_rate"]
    }


@router.post("/production/start")
async def start_production_simulator():
    """Démarre le simulateur de production"""
    if simulator_states["production"]["running"]:
        raise HTTPException(status_code=400, detail="Déjà démarré")
    
    if not kafka_producer:
        raise HTTPException(status_code=503, detail="Kafka non disponible")
    
    simulator_states["production"]["running"] = True
    
    thread = threading.Thread(target=production_simulation_loop, daemon=True)
    thread.start()
    simulator_states["production"]["thread"] = thread
    
    logger.info("▶️  Simulateur de production démarré")
    
    return {
        "success": True,
        "message": "Simulateur de production démarré",
        "config": {"frequency": simulator_states["production"]["frequency"]}
    }


@router.post("/production/stop")
async def stop_production_simulator():
    """Arrête le simulateur de production (Kafka reste actif)"""
    if not simulator_states["production"]["running"]:
        raise HTTPException(status_code=400, detail="N'est pas démarré")
    
    simulator_states["production"]["running"] = False
    
    logger.info("⏸️  Simulateur de production arrêté (Kafka reste actif)")
    
    return {"success": True, "message": "Simulateur arrêté (Kafka actif)"}


@router.put("/production/config")
async def configure_production_simulator(config: SimulatorConfig):
    """Configure le simulateur de production"""
    if config.frequency is not None:
        simulator_states["production"]["frequency"] = max(5, min(60, config.frequency))
    
    return {
        "success": True,
        "message": "Configuration mise à jour",
        "config": {"frequency": simulator_states["production"]["frequency"]}
    }


@router.get("/production/status")
async def get_production_status():
    """Retourne le statut du simulateur de production"""
    return {
        "running": simulator_states["production"]["running"],
        "frequency": simulator_states["production"]["frequency"]
    }


@router.get("/status")
async def get_all_simulators_status():
    """Retourne le statut de tous les simulateurs"""
    return {
        "capteurs": {
            "running": simulator_states["capteurs"]["running"],
            "frequency": simulator_states["capteurs"]["frequency"],
            "error_rate": simulator_states["capteurs"]["error_rate"]
        },
        "production": {
            "running": simulator_states["production"]["running"],
            "frequency": simulator_states["production"]["frequency"]
        }
    }


@router.post("/capteurs/force-error")
async def force_error(req: ForceErrorRequest):
    """Force a breakdown: emit a burst of extreme sensor readings for a machine
    so the ML predictor flags it and the orchestrator stops it (ARRET)."""
    if not kafka_producer:
        raise HTTPException(status_code=503, detail="Kafka non disponible")

    mid = req.machine_id
    # Route through the orchestrator's persistent maintenance listener (reliable),
    # the same channel as Repair. A forced breakdown is a hard fault: ARRET until repair.
    kafka_producer.send('actions-maintenance', value={
        "action": "FORCER_ARRET",
        "machine_id": mid,
        "timestamp": datetime.now().isoformat(),
    })
    kafka_producer.flush()
    logger.warning(f"🔴 Panne forcée sur {mid}")
    return {"success": True, "message": f"Breakdown forced on {mid}"}


def autostart_simulators():
    """Start both generators on boot so the floor is live (and the buttons control them)."""
    if os.getenv("AUTO_START_SIMULATORS", "true").lower() not in ("1", "true", "yes"):
        return
    try:
        if not simulator_states["capteurs"]["running"]:
            simulator_states["capteurs"]["running"] = True
            threading.Thread(target=capteurs_simulation_loop, daemon=True).start()
        if not simulator_states["production"]["running"]:
            simulator_states["production"]["running"] = True
            threading.Thread(target=production_simulation_loop, daemon=True).start()
        logger.info("✅ Simulateurs auto-démarrés")
    except Exception as e:
        logger.error(f"❌ Auto-start error: {e}")


autostart_simulators()