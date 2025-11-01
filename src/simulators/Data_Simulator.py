import json
import time
import random
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealSensorSimulator:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        
        # Initialize Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=[self.bootstrap_servers],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,   # Retry on failure
            request_timeout_ms=30000,
            max_block_ms=30000
        )
        
        self.machines = [
            "POMPE-01", "POMPE-02", "POMPE-03",
            "COMPRESSEUR-01", "COMPRESSEUR-02", 
            "MOTEUR-01", "MOTEUR-02",
            "VENTILATEUR-01", "MIXER-01"
        ]
        
        # Machine-specific base values for realistic data
        self.machine_profiles = {
            "POMPE": {"temp_range": (40, 80), "pressure_range": (100, 200), "vibration_range": (0.5, 2.0)},
            "COMPRESSEUR": {"temp_range": (50, 90), "pressure_range": (150, 300), "vibration_range": (1.0, 3.0)},
            "MOTEUR": {"temp_range": (60, 100), "pressure_range": (0, 50), "vibration_range": (1.5, 4.0)},
            "VENTILATEUR": {"temp_range": (30, 60), "pressure_range": (0, 20), "vibration_range": (0.8, 2.5)},
            "MIXER": {"temp_range": (45, 75), "pressure_range": (80, 180), "vibration_range": (2.0, 5.0)}
        }
        
        logger.info(f"✅ Real Sensor Simulator initialized for Kafka: {bootstrap_servers}")
        logger.info(f"📊 Monitoring {len(self.machines)} machines")
    
    def generate_sensor_data(self, machine_id):
        """Generate realistic sensor data with occasional anomalies"""
        machine_type = machine_id.split('-')[0]
        profile = self.machine_profiles.get(machine_type, self.machine_profiles["POMPE"])
        
        # 97% chance of normal operation, 3% chance of abnormal
        if random.random() > 0.03:
            state = "en_marche" if random.random() > 0.08 else "arret"
            
            # Normal operating ranges
            workload = random.uniform(60.0, 95.0)
            power_consumption = random.uniform(12.0, 22.0)
            vibration = random.uniform(profile["vibration_range"][0], profile["vibration_range"][1])
            temperature = random.uniform(profile["temp_range"][0], profile["temp_range"][1])
            pressure = random.uniform(profile["pressure_range"][0], profile["pressure_range"][1])
        else:
            # Simulate abnormal conditions
            state = random.choice(["maintenance", "panne"])
            workload = random.uniform(0.0, 15.0)
            power_consumption = random.uniform(1.0, 8.0)
            
            # Abnormal readings
            vibration = random.uniform(5.0, 12.0)  # High vibration = potential failure
            temperature = random.uniform(95.0, 130.0)  # Overheating
            pressure = random.uniform(
                profile["pressure_range"][1] + 30, 
                profile["pressure_range"][1] + 120
            )
        
        return {
            "machine_id": machine_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "etat": state,
            "charge_travail": round(workload, 2),
            "consommation_electrique": round(power_consumption, 2),
            "vibration": round(vibration, 2),
            "temperature": round(temperature, 2),
            "pression": round(pressure, 2)
        }
    
    def send_sensor_data(self):
        """Send sensor data for all machines"""
        try:
            for machine in self.machines:
                data = self.generate_sensor_data(machine)
                
                # Send to Kafka with error handling
                future = self.producer.send('donnees-capteurs', data)
                
                # Optional: Wait for send confirmation
                # record_metadata = future.get(timeout=10)
                
                logger.info(f"📨 Sent data for {machine} - État: {data['etat']} - Charge: {data['charge_travail']}%")
                
            # Flush to ensure all messages are sent
            self.producer.flush()
            
        except KafkaError as e:
            logger.error(f"❌ Kafka error: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
    
    def run_continuous_simulation(self, interval=5):
        """Run continuous simulation"""
        logger.info(f"🚀 Starting continuous simulation (interval: {interval}s)")
        logger.info("Press Ctrl+C to stop...")
        
        try:
            while True:
                start_time = time.time()
                self.send_sensor_data()
                
                # Calculate sleep time to maintain exact interval
                processing_time = time.time() - start_time
                sleep_time = max(0, interval - processing_time)
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("🛑 Simulation stopped by user")
        finally:
            self.producer.close()
            logger.info("🔒 Kafka producer closed")

if __name__ == "__main__":
    # Test connection first
    try:
        simulator = RealSensorSimulator('localhost:9092')
        
        # Send one test message
        test_data = simulator.generate_sensor_data("TEST-01")
        future = simulator.producer.send('donnees-capteurs', test_data)
        future.get(timeout=10)  # Wait for confirmation
        logger.info("✅ Connection test successful! Starting simulation...")
        
        # Run continuous simulation
        simulator.run_continuous_simulation(interval=5)
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize: {e}")
        logger.info("💡 Make sure Kafka is running and topics are created")