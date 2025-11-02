"""
Resource Manager V2 - Architecture Modulaire Professionnelle
✅ LIT LES ÉTATS DEPUIS KAFKA (Single Source of Truth)
Auteur: Ahmed Belhouchette (@AhmedBelhouchette10)
Date: 2025-11-02

Architecture:
- models/: Modèles de données (Task, AlerteML, MachineState)
- services/: Services externes (InfluxDB, Kafka, Maintenance)
- managers/: Logique métier (TaskManager, AlertManager, MachineManager)
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from datetime import datetime
import logging
import time

# Imports des modules
from models.machine_state import MachineState
from models.task import Task
from services.influx_service import InfluxService
from services.kafka_service import KafkaService
from services.maintenance_service import MaintenanceService
from managers.alert_manager import AlertManager
from managers.task_manager import TaskManager
from managers.machine_manager import MachineManager

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

# Configuration - DOCKER
KAFKA_BOOTSTRAP = "kafka:29092"
INFLUXDB_URL = "http://influxdb:8086"
INFLUXDB_TOKEN = "my-super-secret-token"
INFLUXDB_ORG = "mon-usine"
INFLUXDB_BUCKET = "donnees-usine"

BATCH_INTERVAL = 30  # secondes

class ResourceManager:
    """Resource Manager Principal - Architecture Modulaire"""
    
    def __init__(self):
        # Machines disponibles
        self.machines = ["POMPE-1", "POMPE-2", "POMPE-3", "POMPE-4", "POMPE-5"]
        
        # Initialiser les services
        logger.info("🔧 Initialisation des services...")
        self.influx_service = InfluxService(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG,
            bucket=INFLUXDB_BUCKET
        )
        
        self.kafka_service = KafkaService(
            bootstrap_servers=KAFKA_BOOTSTRAP
        )
        
        # Initialiser les managers
        logger.info("🔧 Initialisation des managers...")
        self.alert_manager = AlertManager(self.influx_service)
        self.task_manager = TaskManager(self.influx_service, self.kafka_service)
        self.machine_manager = MachineManager(self.machines, self.influx_service)
        
        # Service de maintenance (réparations)
        self.maintenance_service = MaintenanceService(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            callback=self.alert_manager.repair_machine
        )
        self.maintenance_service.start()
        
        # Affichage de démarrage
        self._print_startup_banner()
    
    def _print_startup_banner(self):
        """Affiche la bannière de démarrage"""
        logger.info("")
        logger.info("="*70)
        logger.info("🤖 RESOURCE MANAGER V2 - KAFKA SINGLE SOURCE OF TRUTH")
        logger.info("="*70)
        logger.info(f"👤 Auteur: Ahmed Belhouchette (@AhmedBelhouchette10)")
        logger.info(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("")
        logger.info("📦 Modules chargés:")
        logger.info("   ✅ Services: InfluxDB, Kafka, Maintenance")
        logger.info("   ✅ Managers: Alert, Task, Machine")
        logger.info("   ✅ État des machines: Lecture depuis Kafka")
        logger.info("")
        logger.info("🔧 Configuration:")
        logger.info(f"   • Machines: {len(self.machines)}")
        logger.info(f"   • Batch interval: {BATCH_INTERVAL}s")
        logger.info(f"   • Kafka: {KAFKA_BOOTSTRAP}")
        logger.info(f"   • InfluxDB: {INFLUXDB_URL}")
        logger.info("="*70)
        logger.info("")
    
    def process_batch(self):
        """Traite un batch de tâches et d'alertes"""
        batch_num = getattr(self, 'batch_counter', 0) + 1
        self.batch_counter = batch_num
        
        logger.info("")
        logger.info("#"*70)
        logger.info(f"# BATCH {batch_num} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("#"*70)
        
        # 1. Vérifier les pauses expirées
        self.alert_manager.check_expired_pauses()
        
        # 2. Vérifier les tâches terminées
        completed = self.task_manager.check_completed_tasks()
        for machine_id in completed:
            self.machine_manager.update_activity(machine_id)
        
        # 3. Lire les nouvelles tâches depuis Kafka
        new_tasks = self._read_tasks_from_kafka()
        for task_data in new_tasks:
            self.task_manager.add_task(task_data)
        
        # 4. Lire les alertes ML depuis Kafka
        new_alerts = self._read_alerts_from_kafka()
        for alert_data in new_alerts:
            self._handle_ml_alert(alert_data)
        
        # 5. Vérifier retour d'urgence - ✅ LECTURE DEPUIS KAFKA
        available_count = len(self.machine_manager.get_available_machines(
            self.task_manager.assigned_tasks,
            self.task_manager.get_queue_size()
        ))
        
        self.alert_manager.check_emergency_return(
            available_count,
            self.task_manager.get_queue_size()
        )
        
        # 6. Assigner les tâches en attente
        self._assign_pending_tasks()
        
        # 7. Vérifier machines inactives - ✅ LECTURE DEPUIS KAFKA
        self.machine_manager.check_idle_machines(
            self.task_manager.assigned_tasks,
            self.task_manager.get_queue_size()
        )
        
        # 8. Afficher le résumé
        self._print_batch_summary()
        
        # 9. Écrire les stats dans InfluxDB
        self._write_global_stats()
    
    def _read_tasks_from_kafka(self):
        """Lit les nouvelles tâches depuis Kafka"""
        try:
            consumer = self.kafka_service.create_consumer(
                ['plan-production'],
                'resource-manager-tasks'
            )
            
            messages = consumer.poll(timeout_ms=1000)
            tasks = []
            
            for topic_partition, records in messages.items():
                for record in records:
                    tasks.append(record.value)
            
            consumer.close()
            return tasks
        
        except Exception as e:
            logger.error(f"❌ Erreur lecture tâches: {e}")
            return []
    
    def _read_alerts_from_kafka(self):
        """Lit les alertes ML depuis Kafka"""
        try:
            consumer = self.kafka_service.create_consumer(
                ['alertes-ml'],
                'resource-manager-alerts'
            )
            
            messages = consumer.poll(timeout_ms=1000)
            alerts = []
            
            for topic_partition, records in messages.items():
                for record in records:
                    alerts.append(record.value)
            
            consumer.close()
            return alerts
        
        except Exception as e:
            logger.error(f"❌ Erreur lecture alertes: {e}")
            return []
    
    def _handle_ml_alert(self, alert_data):
        """Traite une alerte ML"""
        machine_id = alert_data.get('machine_id')
        action = alert_data.get('action')
        severity = alert_data.get('severity', 'MOYENNE')
        details = alert_data.get('metrics', {})
        
        # Ajouter l'alerte
        self.alert_manager.add_alert(machine_id, action, severity, str(details))
        
        # Si machine avait une tâche, gérer l'interruption
        if machine_id in self.task_manager.assigned_tasks:
            self.task_manager.handle_machine_failure(machine_id)
    
    def _assign_pending_tasks(self):
        """Assigne les tâches en attente"""
        queue_size = self.task_manager.get_queue_size()
        
        if queue_size == 0:
            return
        
        logger.info(f"📦 {queue_size} tâche(s) à traiter")
        logger.info("")
        
        assignments = 0
        
        while self.task_manager.get_queue_size() > 0:
            # Machines disponibles - ✅ LECTURE DEPUIS KAFKA
            available = self.machine_manager.get_available_machines(
                self.task_manager.assigned_tasks,
                self.task_manager.get_queue_size()
            )
            
            if not available:
                logger.warning(f"   ❌ Aucune machine disponible")
                logger.warning(f"   📥 {self.task_manager.get_queue_size()} tâche(s) en file d'attente")
                break
            
            # Sélectionner la meilleure
            machine_id = self.machine_manager.select_best_machine(available)
            
            # Assigner
            task = self.task_manager.assign_task(machine_id)
            if task:
                assignments += 1
                self.machine_manager.update_activity(machine_id)
                
                # Écrire statut
                self.influx_service.write_machine_status(
                    machine_id,
                    MachineState.ASSIGNEE,
                    f"Tâche {task.task_id}"
                )
        
        logger.info("")
        logger.info(f"✅ {assignments} assignation(s) réussie(s)")
    
    def _print_batch_summary(self):
        """Affiche le résumé du batch"""
        logger.info("")
        logger.info("📊 RÉSUMÉ DU BATCH:")
        
        # Stats tâches
        task_stats = self.task_manager.get_stats()
        logger.info(f"   ✅ Tâches terminées: {task_stats['completed']}")
        logger.info(f"   📥 Tâches en attente: {task_stats['in_queue']}")
        logger.info(f"   ⚠️  Tâches interrompues: {task_stats['interrupted']}")
        
        # Stats machines - ✅ LECTURE DEPUIS KAFKA
        machine_states = self.machine_manager.get_machine_count_by_state(
            self.task_manager.assigned_tasks
        )
        logger.info(f"   🟢 Disponibles: {machine_states['disponible']}")
        logger.info(f"   🔧 Occupées: {machine_states['assignee']}")
        logger.info(f"   🟡 En pause: {machine_states['pause']}")
        logger.info(f"   🔴 En arrêt: {machine_states['arret']}")
        logger.info(f"   💤 En veille: {machine_states['idle']}")
        
        # Stats alertes
        alert_stats = self.alert_manager.get_stats()
        if alert_stats['pauses_expired'] > 0 or alert_stats['emergency_returns'] > 0:
            logger.info(f"   ⏱️  Pauses expirées: {alert_stats['pauses_expired']}")
            logger.info(f"   🚨 Retours urgents: {alert_stats['emergency_returns']}")
    
    def _write_global_stats(self):
        """Écrit les stats globales dans InfluxDB"""
        all_stats = {
            **self.task_manager.get_stats(),
            **self.alert_manager.get_stats()
        }
        self.influx_service.write_stats(all_stats)
    
    def run(self):
        """Boucle principale"""
        logger.info("🚀 Démarrage du Resource Manager...")
        logger.info("")
        
        try:
            while True:
                self.process_batch()
                time.sleep(BATCH_INTERVAL)
        
        except KeyboardInterrupt:
            logger.info("")
            logger.info("="*70)
            logger.info("🛑 ARRÊT DU RESOURCE MANAGER")
            logger.info("="*70)
            self._print_final_stats()
            self.cleanup()
    
    def _print_final_stats(self):
        """Affiche les stats finales"""
        task_stats = self.task_manager.get_stats()
        alert_stats = self.alert_manager.get_stats()
        
        logger.info("")
        logger.info("📊 STATISTIQUES FINALES:")
        logger.info(f"   • Tâches totales: {task_stats['total']}")
        logger.info(f"   • Tâches terminées: {task_stats['completed']}")
        logger.info(f"   • Tâches interrompues: {task_stats['interrupted']}")
        logger.info(f"   • Pauses expirées: {alert_stats['pauses_expired']}")
        logger.info(f"   • Retours urgents: {alert_stats['emergency_returns']}")
        logger.info(f"   • Réparations: {alert_stats['repairs']}")
        logger.info("")
    
    def cleanup(self):
        """Nettoyage avant arrêt"""
        logger.info("🧹 Nettoyage...")
        self.maintenance_service.stop()
        self.kafka_service.close()
        self.influx_service.close()
        logger.info("✅ Arrêt propre terminé")


def main():
    """Point d'entrée principal"""
    rm = ResourceManager()
    rm.run()


if __name__ == "__main__":
    main()