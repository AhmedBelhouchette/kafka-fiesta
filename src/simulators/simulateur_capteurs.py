#!/usr/bin/env python3
"""
Simulateur de capteurs pour machines industrielles
Génère des données réalistes et les envoie au topic Kafka 'donnees-capteurs'
"""

import time
import random
import json
from datetime import datetime
from kafka import KafkaProducer
from typing import Dict, Any
import argparse


class MachineSimulator:
    """Simule une machine industrielle avec ses capteurs"""
    
    # États possibles de la machine
    ETATS = ["en_marche", "en_pause", "arretee"]
    
    # Définition des machines avec leurs plages de valeurs normales
    MACHINES = {
        "POMPE-01": {"base_temp": 45, "base_vibration": 0.8, "base_pression": 150},
        "POMPE-02": {"base_temp": 42, "base_vibration": 0.9, "base_pression": 148},
        "POMPE-03": {"base_temp": 48, "base_vibration": 0.7, "base_pression": 152},
        "POMPE-04": {"base_temp": 44, "base_vibration": 0.85, "base_pression": 149},
        "MOTEUR-01": {"base_temp": 55, "base_vibration": 1.2, "base_pression": 140},
        "MOTEUR-02": {"base_temp": 52, "base_vibration": 1.1, "base_pression": 142},
        "MOTEUR-03": {"base_temp": 58, "base_vibration": 1.3, "base_pression": 138},
        "COMPRESSEUR-01": {"base_temp": 60, "base_vibration": 1.5, "base_pression": 155},
        "COMPRESSEUR-02": {"base_temp": 62, "base_vibration": 1.4, "base_pression": 158},
    }
    
    def __init__(self, machine_id: str):
        self.machine_id = machine_id
        self.config = self.MACHINES.get(machine_id, self.MACHINES["POMPE-01"])
        self.etat = "en_marche"
        self.degradation = 0.0  # Facteur de dégradation (0 = neuf, 1 = panne)
        
    def simulate_normal(self) -> Dict[str, Any]:
        """Simule un fonctionnement normal"""
        charge_travail = random.uniform(60, 95)
        
        return {
            "machine_id": self.machine_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "etat": self.etat,
            "charge_travail": round(charge_travail, 2),
            "consommation_electrique": round(charge_travail * 0.18 + random.uniform(-2, 2), 2),
            "vibration": round(self.config["base_vibration"] + random.uniform(-0.2, 0.2), 3),
            "temperature": round(self.config["base_temp"] + random.uniform(-5, 8), 2),
            "pression": round(self.config["base_pression"] + random.uniform(-5, 5), 2)
        }
    
    def simulate_degraded(self) -> Dict[str, Any]:
        """Simule une machine dégradée (nécessite une pause)"""
        self.degradation = random.uniform(0.3, 0.6)
        charge_travail = random.uniform(80, 98)
        
        return {
            "machine_id": self.machine_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "etat": self.etat,
            "charge_travail": round(charge_travail, 2),
            "consommation_electrique": round(charge_travail * 0.20 + random.uniform(-1, 3), 2),
            "vibration": round(self.config["base_vibration"] * (1 + self.degradation * 1.5) + random.uniform(-0.1, 0.3), 3),
            "temperature": round(self.config["base_temp"] * (1 + self.degradation * 0.4) + random.uniform(0, 10), 2),
            "pression": round(self.config["base_pression"] * (1 - self.degradation * 0.1) + random.uniform(-8, 2), 2)
        }
    
    def simulate_critical(self) -> Dict[str, Any]:
        """Simule une machine en état critique (nécessite une maintenance)"""
        self.degradation = random.uniform(0.7, 1.0)
        charge_travail = random.uniform(95, 100)
        
        return {
            "machine_id": self.machine_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "etat": self.etat,
            "charge_travail": round(charge_travail, 2),
            "consommation_electrique": round(charge_travail * 0.25 + random.uniform(0, 5), 2),
            "vibration": round(self.config["base_vibration"] * (1 + self.degradation * 3) + random.uniform(0, 0.8), 3),
            "temperature": round(self.config["base_temp"] * (1 + self.degradation * 0.6) + random.uniform(5, 15), 2),
            "pression": round(self.config["base_pression"] * (1 - self.degradation * 0.2) + random.uniform(-15, -5), 2)
        }
    
    def generate_reading(self, scenario: str = "random") -> Dict[str, Any]:
        """
        Génère une lecture de capteurs
        
        Args:
            scenario: "normal", "degraded", "critical", ou "random"
        """
        if scenario == "random":
            # Distribution réaliste : 96% normal, 3% dégradé, 1% critique
            # Cela donne un taux d'alertes de 4%
            rand = random.random()
            if rand < 0.96:
                return self.simulate_normal()
            elif rand < 0.99:
                return self.simulate_degraded()
            else:
                return self.simulate_critical()
        elif scenario == "normal":
            return self.simulate_normal()
        elif scenario == "degraded":
            return self.simulate_degraded()
        elif scenario == "critical":
            return self.simulate_critical()


def send_to_kafka(producer: KafkaProducer, topic: str, data: Dict[str, Any]):
    """Envoie des données au topic Kafka"""
    try:
        future = producer.send(topic, value=data)
        future.get(timeout=10)  # Attendre la confirmation
        print(f"[OK] Envoye: {data['machine_id']} - {data['timestamp']}")
    except Exception as e:
        print(f"[ERREUR] Erreur d'envoi: {e}")


def main():
    parser = argparse.ArgumentParser(description="Simulateur de capteurs industriels")
    parser.add_argument("--kafka-server", default="localhost:9092", help="Adresse du serveur Kafka")
    parser.add_argument("--topic", default="donnees-capteurs", help="Topic Kafka")
    parser.add_argument("--interval", type=float, default=2.0, help="Intervalle entre les lectures (secondes)")
    parser.add_argument("--duration", type=int, default=0, help="Durée de simulation (secondes, 0=infini)")
    parser.add_argument("--scenario", choices=["random", "normal", "degraded", "critical"], 
                       default="random", help="Scénario de simulation")
    parser.add_argument("--stdout", action="store_true", help="Afficher sur stdout au lieu de Kafka")
    
    args = parser.parse_args()
    
    # Créer les simulateurs pour toutes les machines
    simulators = [MachineSimulator(machine_id) for machine_id in MachineSimulator.MACHINES.keys()]
    
    # Créer le producteur Kafka
    producer = None
    if not args.stdout:
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.kafka_server,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print(f"Connecte a Kafka: {args.kafka_server}")
            print(f"Topic: {args.topic}")
        except Exception as e:
            print(f"Erreur de connexion a Kafka: {e}")
            print("Mode stdout active")
            args.stdout = True
    
    print(f"Nombre de machines: {len(simulators)}")
    print(f"Scenario: {args.scenario}")
    print(f"Intervalle: {args.interval}s")
    print("="*70)
    print("Demarrage de la simulation... (Ctrl+C pour arreter)")
    print("="*70)
    
    start_time = time.time()
    count = 0
    
    try:
        while True:
            # Vérifier la durée
            if args.duration > 0 and (time.time() - start_time) >= args.duration:
                break
            
            # Générer des lectures pour toutes les machines
            for simulator in simulators:
                reading = simulator.generate_reading(args.scenario)
                
                if args.stdout:
                    print(json.dumps(reading, ensure_ascii=False))
                else:
                    send_to_kafka(producer, args.topic, reading)
                
                count += 1
            
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print("\n" + "="*70)
        print("Arret de la simulation...")
    finally:
        if producer:
            producer.close()
        
        print(f"Total de lectures generees: {count}")
        print("="*70)


if __name__ == "__main__":
    main()