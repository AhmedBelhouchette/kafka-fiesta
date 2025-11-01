"""
Simulateur de test pour le Resource Manager
Envoie des tâches et des alertes ML dans Kafka
"""

from kafka import KafkaProducer
import json
import time
from datetime import datetime
import uuid
import random

# Configuration Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

MACHINES = ["POMPE-1", "POMPE-2", "POMPE-3"]
PRODUITS = ["PRODUIT_A", "PRODUIT_B", "PRODUIT_C", "PRODUIT_D"]

def envoyer_tache():
    """Envoie une tâche dans le plan de production"""
    tache = {
        "tache_id": f"TASK-{uuid.uuid4().hex[:8].upper()}",
        "type_produit": random.choice(PRODUITS),
        "quantite": random.randint(500, 5000)
    }
    
    producer.send('plan-de-production', value=tache)
    producer.flush()
    
    print(f"📋 Tâche envoyée: {tache['tache_id']} - {tache['type_produit']} x{tache['quantite']}")
    return tache

def envoyer_alerte_ml(machine_id, force_action=None):
    """Envoie une alerte ML pour une machine"""
    if force_action:
        if force_action == "CONTINUER":
            alerte_type = {
                "type_panne": "aucune",
                "probabilite_panne": random.uniform(0.0, 0.3),
                "action_recommandee": "CONTINUER"
            }
        elif force_action == "PAUSE":
            alerte_type = {
                "type_panne": "pause_requise",
                "probabilite_panne": random.uniform(0.5, 0.7),
                "action_recommandee": "PAUSE"
            }
        else:  # ARRETER
            alerte_type = {
                "type_panne": "maintenance_requise",
                "probabilite_panne": random.uniform(0.75, 0.95),
                "action_recommandee": "ARRETER"
            }
    else:
        # Simuler différents types d'alertes
        alertes_possibles = [
            {
                "type_panne": "aucune",
                "probabilite_panne": random.uniform(0.0, 0.3),
                "action_recommandee": "CONTINUER"
            },
            {
                "type_panne": "pause_requise",
                "probabilite_panne": random.uniform(0.5, 0.7),
                "action_recommandee": "PAUSE"
            },
            {
                "type_panne": "maintenance_requise",
                "probabilite_panne": random.uniform(0.75, 0.95),
                "action_recommandee": "ARRETER"
            }
        ]
        alerte_type = random.choice(alertes_possibles)
    
    alerte = {
        "alert_id": str(uuid.uuid4()),
        "machine_id": machine_id,
        "timestamp_detection": datetime.utcnow().isoformat() + "Z",
        "type_panne": alerte_type["type_panne"],
        "probabilite_panne": alerte_type["probabilite_panne"],
        "metriques_actuelles": {
            "vibration": random.uniform(1.0, 5.0),
            "temperature": random.uniform(60.0, 95.0),
            "pression": random.uniform(1.0, 3.0),
            "consommation_electrique": random.uniform(5.0, 20.0),
            "charge_travail": random.uniform(0, 100)
        },
        "action_recommandee": alerte_type["action_recommandee"],
        "features_ml": {
            "vibration_moyenne_1min": random.uniform(2.0, 4.0),
            "temperature_moyenne_5min": random.uniform(70.0, 85.0),
            "pression_ecart_type_10min": random.uniform(0.1, 0.5)
        }
    }
    
    producer.send('alertes-ml', value=alerte)
    producer.flush()
    
    emoji = "🟢" if alerte_type["action_recommandee"] == "CONTINUER" else "🟡" if alerte_type["action_recommandee"] == "PAUSE" else "🔴"
    print(f"{emoji} Alerte ML: {machine_id} - {alerte_type['action_recommandee']} (prob: {alerte_type['probabilite_panne']:.2f})")
    
    return alerte

def scenario_test_complet():
    """
    Scénario de test complet pour le Resource Manager
    """
    print("\n" + "="*60)
    print("🧪 DÉBUT DU SCÉNARIO DE TEST")
    print("="*60 + "\n")
    
    # Étape 1: Envoyer des alertes ML pour toutes les machines (toutes disponibles)
    print("\n--- ÉTAPE 1: Toutes les machines disponibles ---")
    for machine in MACHINES:
        envoyer_alerte_ml(machine, force_action="CONTINUER")
        time.sleep(0.5)
    
    time.sleep(3)
    
    # Étape 2: Envoyer 3 tâches (une pour chaque machine)
    print("\n--- ÉTAPE 2: Envoi de 3 tâches ---")
    for i in range(3):
        envoyer_tache()
        time.sleep(2)
    
    time.sleep(10)
    
    # Étape 3: Mettre une machine en alerte ARRÊT
    print("\n--- ÉTAPE 3: POMPE-1 en alerte ARRÊT ---")
    envoyer_alerte_ml("POMPE-1", force_action="ARRETER")
    print("🔴 POMPE-1 marquée comme ARRÊTER")
    
    time.sleep(5)
    
    # Étape 4: Envoyer 2 tâches supplémentaires
    print("\n--- ÉTAPE 4: Envoi de 2 tâches (POMPE-1 indisponible) ---")
    for i in range(2):
        envoyer_tache()
        time.sleep(2)
    
    time.sleep(10)
    
    # Étape 5: Réactiver POMPE-1
    print("\n--- ÉTAPE 5: POMPE-1 de nouveau disponible ---")
    envoyer_alerte_ml("POMPE-1", force_action="CONTINUER")
    print("🟢 POMPE-1 de nouveau opérationnelle")
    
    time.sleep(5)
    
    # Étape 6: Envoyer une grosse tâche
    print("\n--- ÉTAPE 6: Grosse tâche (10000 unités) ---")
    grosse_tache = {
        "tache_id": f"TASK-BIG-{uuid.uuid4().hex[:4].upper()}",
        "type_produit": "PRODUIT_SPECIAL",
        "quantite": 10000
    }
    producer.send('plan-de-production', value=grosse_tache)
    producer.flush()
    print(f"📋 GROSSE tâche envoyée: {grosse_tache['tache_id']} - {grosse_tache['quantite']} unités")
    
    print("\n" + "="*60)
    print("✅ SCÉNARIO DE TEST TERMINÉ")
    print("="*60 + "\n")

def mode_continu():
    """
    Mode continu: envoie des tâches et alertes régulièrement
    """
    print("\n🔄 MODE CONTINU ACTIVÉ")
    print("Envoi de tâches toutes les 20 secondes...")
    print("Envoi d'alertes ML toutes les 30 secondes...")
    print("Appuyez sur Ctrl+C pour arrêter\n")
    
    compteur_taches = 0
    compteur_alertes = 0
    
    try:
        while True:
            # Envoyer une tâche
            if compteur_taches % 20 == 0:
                envoyer_tache()
            
            # Envoyer une alerte ML aléatoire
            if compteur_alertes % 30 == 0:
                machine = random.choice(MACHINES)
                envoyer_alerte_ml(machine)
            
            compteur_taches += 1
            compteur_alertes += 1
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Arrêt du simulateur")

if __name__ == "__main__":
    print("="*60)
    print("🧪 SIMULATEUR DE TEST - RESOURCE MANAGER")
    print("="*60)
    print("\nChoisissez un mode:")
    print("1. Scénario de test complet (automatique)")
    print("2. Mode continu (envoie régulier)")
    print("3. Envoi manuel")
    
    choix = input("\nVotre choix (1/2/3): ").strip()
    
    if choix == "1":
        scenario_test_complet()
    elif choix == "2":
        mode_continu()
    elif choix == "3":
        while True:
            print("\n--- MENU MANUEL ---")
            print("1. Envoyer une tâche")
            print("2. Envoyer une alerte ML")
            print("3. Quitter")
            
            action = input("Action: ").strip()
            
            if action == "1":
                envoyer_tache()
            elif action == "2":
                print("Machines disponibles:", MACHINES)
                machine = input("Machine ID: ").strip() or random.choice(MACHINES)
                envoyer_alerte_ml(machine)
            elif action == "3":
                break
    else:
        print("❌ Choix invalide")