"""
Simulateur de Capteurs Intelligent
Compatible avec Resource Manager - Ahmed Belhouchette (@AhmedBelhouchette10)
Date: 2025-11-02
"""

import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
from influxdb_client import InfluxDBClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SmartSensorSimulator:
    """Simulateur qui respecte l'état des machines"""
    
    def __init__(self):
        self.running = False
        self.frequency = 5  # secondes
        self.error_rate = 0.15  # 15% de chance d'erreur
        self.machines = ["POMPE-1", "POMPE-2", "POMPE-3", "POMPE-4", "POMPE-5"]
        
        # Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # InfluxDB Client
        self.influx_client = InfluxDBClient(
            url="http://localhost:8086",
            token="my-super-secret-token",
            org="mon-usine"
        )
        self.query_api = self.influx_client.query_api()
        
        logger.info("✅ Simulateur de capteurs initialisé")
    
    def get_machine_status(self, machine_id):
        """Lit l'état actuel de la machine depuis InfluxDB"""
        try:
            query = f'''
            from(bucket: "donnees-usine")
                |> range(start: -10m)
                |> filter(fn: (r) => r["_measurement"] == "statut_machines")
                |> filter(fn: (r) => r["machine_id"] == "{machine_id}")
                |> filter(fn: (r) => r["_field"] == "statut")
                |> last()
            '''
            
            result = self.query_api.query(query)
            
            for table in result:
                for record in table.records:
                    return record.get_value()
            
            return "DISPONIBLE"  # Par défaut
        
        except Exception as e:
            logger.warning(f"⚠️  Erreur lecture statut {machine_id}: {e}")
            return "DISPONIBLE"
    
    def should_send_data(self, status):
        """Décide si on doit envoyer des données pour cette machine"""
        # N'envoyer QUE si machine active
        return status in ["DISPONIBLE", "ASSIGNEE"]
    
    def generate_sensor_data(self, machine_id, force_error=False):
        """Génère des données de capteurs réalistes"""
        
        # Mode NORMAL (pas d'erreur)
        if not force_error and random.random() > self.error_rate:
            temperature = random.uniform(55, 75)
            charge = random.uniform(25, 70)
            vibration = random.uniform(0.5, 2.0)
        
        # Mode ERREUR (données anormales)
        else:
            error_type = random.choice(['temperature', 'charge', 'both'])
            
            if error_type == 'temperature':
                temperature = random.uniform(80, 95)
                charge = random.uniform(25, 70)
            elif error_type == 'charge':
                temperature = random.uniform(55, 75)
                charge = random.uniform(75, 95)
            else:  # both
                temperature = random.uniform(80, 95)
                charge = random.uniform(75, 95)
            
            vibration = random.uniform(2.5, 4.0)
        
        return {
            "machine_id": machine_id,
            "timestamp": datetime.now().isoformat(),
            "temperature": round(temperature, 2),
            "vibration": round(vibration, 3),
            "pression": round(random.uniform(3.5, 6.5), 2),
            "consommation_electrique": round(random.uniform(80, 150), 2),
            "charge_travail": round(charge, 2)
        }
    
    def force_machine_error(self, machine_id):
        """Force une erreur sur une machine spécifique"""
        logger.warning(f"🔴 Forçage d'erreur sur {machine_id}")
        data = self.generate_sensor_data(machine_id, force_error=True)
        self.producer.send('donnees-capteurs', value=data)
        self.producer.flush()
        logger.info(f"📤 Données anormales envoyées: T={data['temperature']}°C, C={data['charge_travail']}%")
    
    def run(self):
        """Boucle principale du simulateur"""
        logger.info("="*70)
        logger.info("🚀 SIMULATEUR DE CAPTEURS - DÉMARRÉ")
        logger.info("="*70)
        logger.info(f"⏱️  Fréquence: {self.frequency} secondes")
        logger.info(f"⚠️  Taux d'erreurs: {self.error_rate*100:.0f}%")
        logger.info(f"📡 Topic Kafka: donnees-capteurs")
        logger.info("="*70)
        
        self.running = True
        cycle = 0
        
        try:
            while self.running:
                cycle += 1
                logger.info(f"\n📊 CYCLE #{cycle}")
                
                for machine_id in self.machines:
                    # Lire l'état actuel
                    status = self.get_machine_status(machine_id)
                    
                    # N'envoyer QUE si machine active
                    if self.should_send_data(status):
                        data = self.generate_sensor_data(machine_id)
                        self.producer.send('donnees-capteurs', value=data)
                        
                        # Log avec emoji selon valeurs
                        temp_emoji = "🔥" if data['temperature'] > 80 else "🌡️"
                        charge_emoji = "⚡" if data['charge_travail'] > 75 else "💪"
                        
                        logger.info(
                            f"  ✅ {machine_id:10} ({status:10}): "
                            f"{temp_emoji} T={data['temperature']:5.1f}°C  "
                            f"{charge_emoji} C={data['charge_travail']:5.1f}%"
                        )
                    else:
                        logger.info(f"  ⏸️  {machine_id:10} ({status:10}): Pas d'envoi de données")
                
                self.producer.flush()
                time.sleep(self.frequency)
        
        except KeyboardInterrupt:
            logger.info("\n🛑 Arrêt demandé par l'utilisateur")
        finally:
            self.stop()
    
    def stop(self):
        """Arrête proprement le simulateur"""
        self.running = False
        self.producer.close()
        self.influx_client.close()
        logger.info("🛑 Simulateur arrêté proprement")
    
    def set_frequency(self, frequency):
        """Change la fréquence d'envoi"""
        self.frequency = max(1, min(30, frequency))
        logger.info(f"⏱️  Nouvelle fréquence: {self.frequency}s")
    
    def set_error_rate(self, rate):
        """Change le taux d'erreurs (0.0 à 1.0)"""
        self.error_rate = max(0.0, min(1.0, rate))
        logger.info(f"⚠️  Nouveau taux d'erreurs: {self.error_rate*100:.0f}%")


# Instance globale pour contrôle via API
simulator_instance = None

def get_simulator():
    """Retourne l'instance du simulateur (singleton)"""
    global simulator_instance
    if simulator_instance is None:
        simulator_instance = SmartSensorSimulator()
    return simulator_instance


def main():
    """Point d'entrée"""
    simulator = SmartSensorSimulator()
    simulator.run()


if __name__ == "__main__":
    main()