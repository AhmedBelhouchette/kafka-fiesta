"""
Service pour gérer les interactions avec InfluxDB
"""

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import logging

logger = logging.getLogger(__name__)

class InfluxService:
    """Gestion des opérations InfluxDB"""
    
    def __init__(self, url, token, org, bucket):
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        
        logger.info(f"✅ InfluxDB connecté: {url}")
    
    def get_all_machine_statuses(self):
        """Latest status of every machine from the statut_machines measurement.

        This is the real single source of truth for machine status (it's where
        every write_machine_status lands), so availability is read from here.
        """
        statuses = {}
        try:
            query = f'''
            from(bucket: "{self.bucket}")
                |> range(start: -15m)
                |> filter(fn: (r) => r["_measurement"] == "statut_machines")
                |> filter(fn: (r) => r["_field"] == "statut")
                |> group(columns: ["machine_id"])
                |> last()
            '''
            for table in self.query_api.query(query):
                for record in table.records:
                    statuses[record.values.get("machine_id")] = record.get_value()
        except Exception as e:
            logger.error(f"❌ Erreur lecture statuts machines: {e}")
        return statuses

    def get_machine_state(self, machine_id):
        """Récupère l'état actuel d'une machine depuis InfluxDB"""
        try:
            query = f'''
            from(bucket: "{self.bucket}")
                |> range(start: -1m)
                |> filter(fn: (r) => r["_measurement"] == "etat_machines")
                |> filter(fn: (r) => r["machine_id"] == "{machine_id}")
                |> filter(fn: (r) => r["_field"] == "temperature" or r["_field"] == "charge_travail")
                |> last()
            '''
            
            result = self.query_api.query(query)
            
            state = {}
            for table in result:
                for record in table.records:
                    field = record.get_field()
                    value = record.get_value()
                    state[field] = value
            
            if state:
                logger.debug(f"État {machine_id}: T={state.get('temperature', 0):.1f}°C, Charge={state.get('charge_travail', 0):.1f}%")
            
            return state
        
        except Exception as e:
            logger.error(f"❌ Erreur lecture InfluxDB pour {machine_id}: {e}")
            return {}
    
    def write_machine_status(self, machine_id, status, details=""):
        """Écrit le statut d'une machine dans InfluxDB"""
        try:
            point = Point("statut_machines") \
                .tag("machine_id", machine_id) \
                .field("statut", status) \
                .field("details", details)
            
            self.write_api.write(bucket=self.bucket, record=point)
            logger.debug(f"📝 Statut écrit: {machine_id} → {status}")
        
        except Exception as e:
            logger.error(f"❌ Erreur écriture statut {machine_id}: {e}")
    
    def write_task_assignment(self, machine_id, task_id, product_name, duration_minutes):
        """Écrit une assignation de tâche dans InfluxDB"""
        try:
            point = Point("assignations") \
                .tag("machine_id", machine_id) \
                .tag("task_id", task_id) \
                .field("product_name", product_name) \
                .field("duration_minutes", duration_minutes)
            
            self.write_api.write(bucket=self.bucket, record=point)
            logger.debug(f"📝 Assignation écrite: {task_id} → {machine_id}")
        
        except Exception as e:
            logger.error(f"❌ Erreur écriture assignation: {e}")
    
    def write_stats(self, stats):
        """Écrit les statistiques globales"""
        try:
            point = Point("resource_manager_stats")
            
            for key, value in stats.items():
                point.field(key, value)
            
            self.write_api.write(bucket=self.bucket, record=point)
            logger.debug(f"📊 Stats écrites: {stats}")
        
        except Exception as e:
            logger.error(f"❌ Erreur écriture stats: {e}")
    
    def write_queue_size(self, queue_size):
        """Écrit la taille de la file d'attente"""
        try:
            point = Point("task_queue") \
                .field("size", queue_size)
            
            self.write_api.write(bucket=self.bucket, record=point)
        
        except Exception as e:
            logger.error(f"❌ Erreur écriture queue: {e}")
    
    def close(self):
        """Ferme la connexion InfluxDB"""
        self.client.close()
        logger.info("InfluxDB connexion fermée")