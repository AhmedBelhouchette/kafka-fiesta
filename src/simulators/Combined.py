import threading
import time
import logging
from Data_Simulator import RealSensorSimulator
from Plan_Simulator import RealProductionSimulator

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RealCombinedSimulator:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.sensor_simulator = RealSensorSimulator(bootstrap_servers)
        self.production_simulator = RealProductionSimulator(bootstrap_servers)
        
    def start_all_simulations(self):
        """Start both sensor and production simulations in separate threads"""
        logger.info("🚀 Starting Combined Big Data Simulator")
        logger.info("=" * 60)
        logger.info("📊 Sensor Data: Every 5 seconds")
        logger.info("🏭 Production Plans: Every 30 seconds")
        logger.info("⏹️  Press Ctrl+C to stop all simulations")
        logger.info("=" * 60)
        
        try:
            # Start sensor data simulation
            sensor_thread = threading.Thread(
                target=self.sensor_simulator.run_continuous_simulation,
                args=(5,),
                daemon=True,
                name="SensorSimulator"
            )
            
            # Start production plan simulation
            production_thread = threading.Thread(
                target=self.production_simulator.run_continuous_simulation,
                args=(30,),
                daemon=True,
                name="ProductionSimulator"
            )
            
            sensor_thread.start()
            production_thread.start()
            
            # Keep main thread alive
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("🛑 All simulations stopped by user")
        except Exception as e:
            logger.error(f"❌ Simulation error: {e}")

if __name__ == "__main__":
    simulator = RealCombinedSimulator('localhost:9092')
    simulator.start_all_simulations()