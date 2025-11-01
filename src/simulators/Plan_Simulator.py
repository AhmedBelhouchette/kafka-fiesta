import json
import time
import random
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealProductionSimulator:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        
        self.producer = KafkaProducer(
            bootstrap_servers=[self.bootstrap_servers],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )
        
        self.product_types = ["PRODUIT_A", "PRODUIT_B", "PRODUIT_C", "PRODUIT_D", "PRODUIT_E"]
        self.task_counter = 1000
        
        logger.info(f"✅ Production Plan Simulator initialized for Kafka: {bootstrap_servers}")
    
    def generate_production_task(self):
        """Generate a production task with realistic quantities"""
        task = {
            "tache_id": f"TASK-{self.task_counter:04d}{random.choice('ABCDEF')}",
            "type_produit": random.choice(self.product_types),
            "quantite": random.choice([1000, 2000, 3000, 5000, 7500, 10000]),
            "timestamp_creation": datetime.now().isoformat()
        }
        self.task_counter += 1
        return task
    
    def send_production_plan(self):
        """Send a production plan to Kafka"""
        try:
            task = self.generate_production_task()
            
            future = self.producer.send('plan-de-production', task)
            # record_metadata = future.get(timeout=10)
            
            logger.info(f"🏭 Sent production task: {task['tache_id']} - Produit: {task['type_produit']} - Quantité: {task['quantite']}")
            
            self.producer.flush()
            
        except KafkaError as e:
            logger.error(f"❌ Kafka error: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
    
    def run_continuous_simulation(self, interval=30):
        """Run continuous simulation"""
        logger.info(f"🏭 Starting production plan simulation (interval: {interval}s)")
        logger.info("Press Ctrl+C to stop...")
        
        try:
            while True:
                self.send_production_plan()
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("🛑 Production simulation stopped by user")
        finally:
            self.producer.close()
            logger.info("🔒 Kafka producer closed")

if __name__ == "__main__":
    try:
        simulator = RealProductionSimulator('localhost:9092')
        simulator.run_continuous_simulation(interval=30)
    except Exception as e:
        logger.error(f"❌ Failed to initialize: {e}")