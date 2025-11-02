"""
Simulateur de Plan de Production Intelligent
Compatible avec Resource Manager - Ahmed Belhouchette (@AhmedBelhouchette10)
Date: 2025-11-02
"""

import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SmartProductionSimulator:
    """Génère des tâches de production de manière intelligente"""
    
    def __init__(self):
        self.running = False
        self.frequency = 10  # secondes entre chaque tâche
        self.task_counter = 1040  # Commence à TASK-1040
        
        # Types de produits avec durées
        self.products = {
            "Produit A": {"duration": 30, "priority": "NORMALE"},
            "Produit B": {"duration": 45, "priority": "NORMALE"},
            "Produit C": {"duration": 60, "priority": "NORMALE"},
            "Produit D": {"duration": 90, "priority": "HAUTE"},
            "Produit E": {"duration": 120, "priority": "HAUTE"}
        }
        
        # Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        logger.info("✅ Simulateur de production initialisé")
    
    def generate_task_id(self):
        """Génère un ID de tâche unique"""
        self.task_counter += 1
        suffix = random.choice(['A', 'B', 'C', 'D', 'E', 'F'])
        return f"TASK-{self.task_counter}{suffix}"
    
    def generate_task(self):
        """Génère une nouvelle tâche de production"""
        product_name = random.choice(list(self.products.keys()))
        product_info = self.products[product_name]
        
        task = {
            "task_id": self.generate_task_id(),
            "product": product_name,
            "duration_minutes": product_info["duration"],
            "priority": product_info["priority"],
            "timestamp": datetime.now().isoformat(),
            "status": "EN_ATTENTE"
        }
        
        return task
    
    def run(self):
        """Boucle principale du simulateur"""
        logger.info("="*70)
        logger.info("📦 SIMULATEUR DE PRODUCTION - DÉMARRÉ")
        logger.info("="*70)
        logger.info(f"⏱️  Fréquence: {self.frequency} secondes")
        logger.info(f"📡 Topic Kafka: plan-production")
        logger.info("="*70)
        
        self.running = True
        
        try:
            while self.running:
                # Générer une nouvelle tâche
                task = self.generate_task()
                
                # Envoyer vers Kafka
                self.producer.send('plan-production', value=task)
                self.producer.flush()
                
                # Log
                priority_emoji = "🔴" if task["priority"] == "HAUTE" else "🟡"
                logger.info(
                    f"📦 Nouvelle tâche: {task['task_id']:12} | "
                    f"{task['product']:10} | "
                    f"{priority_emoji} {task['priority']:8} | "
                    f"⏱️  {task['duration_minutes']} min"
                )
                
                time.sleep(self.frequency)
        
        except KeyboardInterrupt:
            logger.info("\n🛑 Arrêt demandé par l'utilisateur")
        finally:
            self.stop()
    
    def stop(self):
        """Arrête proprement le simulateur"""
        self.running = False
        self.producer.close()
        logger.info("🛑 Simulateur arrêté proprement")
    
    def set_frequency(self, frequency):
        """Change la fréquence de génération de tâches"""
        self.frequency = max(5, min(60, frequency))
        logger.info(f"⏱️  Nouvelle fréquence: {self.frequency}s")


# Instance globale pour contrôle via API
production_simulator_instance = None

def get_production_simulator():
    """Retourne l'instance du simulateur (singleton)"""
    global production_simulator_instance
    if production_simulator_instance is None:
        production_simulator_instance = SmartProductionSimulator()
    return production_simulator_instance


def main():
    """Point d'entrée"""
    simulator = SmartProductionSimulator()
    simulator.run()


if __name__ == "__main__":
    main()