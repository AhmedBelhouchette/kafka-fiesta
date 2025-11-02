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
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("✅ Kafka Producer PERMANENT initialisé")
        except Exception as e:
            logger.error(f"❌ Erreur Kafka Producer: {e}")
    
    if influx_client is None:
        try:
            influx_client = InfluxDBClient(
                url="http://localhost:8086",
                token="my-super-secret-token",
                org="mon-usine"
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
        "frequency": 5,
        "error_rate": 0.15
    },
    "production": {
        "running": False,
        "thread": None,
        "frequency": 10,
        "task_counter": 1040
    }
}

# Modèles Pydantic
class SimulatorConfig(BaseModel):
    frequency: int = None
    error_rate: float = None

class ForceErrorRequest(BaseModel):
    machine_id: str


# ==================== BOUCLES DE SIMULATION ====================

def capteurs_simulation_loop():
    """
    Boucle de simulation des capteurs
    Utilise le Producer Kafka PERMANENT
    """
    logger.info("🚀 Thread simulateur capteurs démarré")
    
    machines = ["POMPE-1", "POMPE-2", "POMPE-3", "POMPE-4", "POMPE-5"]
    cycle = 0
    
    while simulator_states["capteurs"]["running"]:
        cycle += 1
        logger.info(f"\n📊 CYCLE #{cycle}")
        
        for machine_id in machines:
            try:
                # Lire statut depuis InfluxDB
                status = "DISPONIBLE"
                if influx_query_api:
                    query = f'''
                    from(bucket: "donnees-usine")
                        |> range(start: -10m)
                        |> filter(fn: (r) => r["_measurement"] == "statut_machines")
                        |> filter(fn: (r) => r["machine_id"] == "{machine_id}")
                        |> filter(fn: (r) => r["_field"] == "statut")
                        |> last()
                    '''
                    result = influx_query_api.query(query)
                    
                    for table in result:
                        for record in table.records:
                            status = record.get_value()
                            break
                
                # N'envoyer QUE si machine active
                if status in ["DISPONIBLE", "ASSIGNEE"]:
                    # Générer données
                    error_rate = simulator_states["capteurs"]["error_rate"]
                    force_error = random.random() < error_rate
                    
                    if force_error:
                        temperature = random.uniform(80, 95)
                        charge = random.uniform(75, 95)
                    else:
                        temperature = random.uniform(55, 75)
                        charge = random.uniform(25, 70)
                    
                    data = {
                        "machine_id": machine_id,
                        "timestamp": datetime.now().isoformat(),
                        "temperature": round(temperature, 2),
                        "vibration": round(random.uniform(0.5, 3.0), 3),
                        "pression": round(random.uniform(3.5, 6.5), 2),
                        "consommation_electrique": round(random.uniform(80, 150), 2),
                        "charge_travail": round(charge, 2)
                    }
                    
                    # Envoyer via Kafka PERMANENT
                    if kafka_producer:
                        kafka_producer.send('donnees-capteurs', value=data)
                    
                    temp_emoji = "🔥" if temperature > 80 else "🌡️"
                    charge_emoji = "⚡" if charge > 75 else "💪"
                    
                    logger.info(
                        f"  ✅ {machine_id:10} ({status:10}): "
                        f"{temp_emoji} T={temperature:5.1f}°C  "
                        f"{charge_emoji} C={charge:5.1f}%"
                    )
                else:
                    logger.info(f"  ⏸️  {machine_id:10} ({status:10}): Pas d'envoi")
            
            except Exception as e:
                logger.warning(f"⚠️  Erreur {machine_id}: {e}")
        
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
        "Produit A": {"duration": 30, "priority": "NORMALE"},
        "Produit B": {"duration": 45, "priority": "NORMALE"},
        "Produit C": {"duration": 60, "priority": "NORMALE"},
        "Produit D": {"duration": 90, "priority": "HAUTE"},
        "Produit E": {"duration": 120, "priority": "HAUTE"}
    }
    
    while simulator_states["production"]["running"]:
        # Générer tâche
        simulator_states["production"]["task_counter"] += 1
        task_id = f"TASK-{simulator_states['production']['task_counter']}{random.choice(['A','B','C','D','E','F'])}"
        
        product_name = random.choice(list(products.keys()))
        product_info = products[product_name]
        
        task = {
            "task_id": task_id,
            "product": product_name,
            "duration_minutes": product_info["duration"],
            "priority": product_info["priority"],
            "timestamp": datetime.now().isoformat(),
            "status": "EN_ATTENTE"
        }
        
        # Envoyer via Kafka PERMANENT
        if kafka_producer:
            kafka_producer.send('plan-production', value=task)
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