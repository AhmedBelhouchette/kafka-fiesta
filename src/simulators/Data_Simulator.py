"""
Enhanced Data Simulator - Écrit dans Kafka ET InfluxDB
Compatible avec le Resource Manager d'Ahmed
"""

import json
import time
import random
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealSensorSimulator:
    def __init__(self, bootstrap_servers='localhost:9092', 
                 influxdb_url='http://localhost:8086',
                 influxdb_token='my-super-secret-token',
                 influxdb_org='mon-usine',
                 influxdb_bucket='donnees-usine'):
        
        self.bootstrap_servers = bootstrap_servers

        # Initialize Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=[self.bootstrap_servers],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks='all',
            retries=3,
            request_timeout_ms=30000,
            max_block_ms=30000
        )

        # Initialize InfluxDB Client
        self.influx_client = InfluxDBClient(
            url=influxdb_url,
            token=influxdb_token,
            org=influxdb_org
        )
        self.influx_write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        self.influx_bucket = influxdb_bucket

        # Machines compatibles avec le Resource Manager d'Ahmed
        self.machines = [
            "POMPE-1", "POMPE-2", "POMPE-3", "POMPE-4", "POMPE-5"
        ]

        # Machine-specific base values for realistic data
        self.machine_profiles = {
            "POMPE": {"temp_range": (65, 85), "pressure_range": (100, 200), "vibration_range": (1.5, 3.5)}
        }

        logger.info(f"✅ Enhanced Sensor Simulator initialized")
        logger.info(f"   📡 Kafka: {bootstrap_servers}")
        logger.info(f"   💾 InfluxDB: {influxdb_url}")
        logger.info(f"   🔧 Machines: {len(self.machines)}")

    def generate_sensor_data(self, machine_id):
        """Generate realistic sensor data"""
        machine_type = machine_id.split('-')[0]
        profile = self.machine_profiles.get(machine_type, self.machine_profiles["POMPE"])

        # 95% chance of normal operation
        if random.random() > 0.05:
            state = "operationnelle"
            workload = random.uniform(30.0, 80.0)
            power_consumption = random.uniform(8.0, 15.0)
            vibration = random.uniform(profile["vibration_range"][0], profile["vibration_range"][1])
            temperature = random.uniform(profile["temp_range"][0], profile["temp_range"][1])
            pressure = random.uniform(profile["pressure_range"][0], profile["pressure_range"][1])
        else:
            # Abnormal conditions
            state = random.choice(["maintenance", "panne"])
            workload = random.uniform(0.0, 20.0)
            power_consumption = random.uniform(1.0, 5.0)
            vibration = random.uniform(5.0, 10.0)
            temperature = random.uniform(90.0, 120.0)
            pressure = random.uniform(250, 350)

        return {
            "machine_id": machine_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "etat": state,
            "charge_travail": round(workload, 2),
            "consommation_electrique": round(power_consumption, 2),
            "vibration": round(vibration, 2),
            "temperature": round(temperature, 2),
            "pression": round(pressure, 2),
            "nombre_taches": random.randint(0, 3)
        }

    def send_to_kafka(self, data):
        """Send data to Kafka"""
        try:
            future = self.producer.send('donnees-capteurs', data)
            return True
        except KafkaError as e:
            logger.error(f"❌ Kafka error for {data['machine_id']}: {e}")
            return False

    def send_to_influxdb(self, data):
        """Send data to InfluxDB (measurement: etat_machines)"""
        try:
            point = Point("etat_machines") \
                .tag("machine_id", data["machine_id"]) \
                .field("temperature", data["temperature"]) \
                .field("vibration", data["vibration"]) \
                .field("charge_travail", data["charge_travail"]) \
                .field("consommation_electrique", data["consommation_electrique"]) \
                .field("etat", data["etat"]) \
                .field("nombre_taches", data["nombre_taches"]) \
                .time(datetime.utcnow())
            
            self.influx_write_api.write(bucket=self.influx_bucket, record=point)
            return True
        except Exception as e:
            logger.error(f"❌ InfluxDB error for {data['machine_id']}: {e}")
            return False

    def send_sensor_data(self):
        """Send sensor data for all machines to Kafka AND InfluxDB"""
        try:
            for machine in self.machines:
                data = self.generate_sensor_data(machine)

                # Send to both Kafka and InfluxDB
                kafka_ok = self.send_to_kafka(data)
                influx_ok = self.send_to_influxdb(data)

                status = "✅" if (kafka_ok and influx_ok) else "⚠️"
                emoji = "🟢" if data['etat'] == "operationnelle" else "🟡" if data['etat'] == "maintenance" else "🔴"
                
                logger.info(f"{status} {emoji} {machine}: temp={data['temperature']}°C, charge={data['charge_travail']}%, état={data['etat']}")

            # Flush Kafka
            self.producer.flush()

        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")

    def run_continuous_simulation(self, interval=10):
        """Run continuous simulation"""
        logger.info(f"🚀 Starting enhanced simulation (interval: {interval}s)")
        logger.info("   📡 Writing to Kafka: donnees-capteurs")
        logger.info("   💾 Writing to InfluxDB: etat_machines")
        logger.info("Press Ctrl+C to stop...")
        logger.info("=" * 60)

        try:
            cycle = 0
            while True:
                start_time = time.time()
                
                logger.info(f"\n--- CYCLE #{cycle} - {datetime.now().strftime('%H:%M:%S')} ---")
                self.send_sensor_data()
                
                processing_time = time.time() - start_time
                sleep_time = max(0, interval - processing_time)
                time.sleep(sleep_time)
                
                cycle += 1

        except KeyboardInterrupt:
            logger.info("\n🛑 Simulation stopped by user")
        finally:
            self.producer.close()
            self.influx_client.close()
            logger.info("🔒 Connections closed")

if __name__ == "__main__":
    try:
        simulator = RealSensorSimulator('localhost:9092')
        logger.info("✅ Connection test successful! Starting simulation...")
        simulator.run_continuous_simulation(interval=10)

    except Exception as e:
        logger.error(f"❌ Failed to initialize: {e}")
        logger.info("💡 Make sure Kafka and InfluxDB are running")
        import traceback
        traceback.print_exc()