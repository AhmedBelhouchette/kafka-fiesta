"""
Gestionnaire des tâches et de la file d'attente
"""

from datetime import datetime
import logging
from models.task import Task
from models.machine_state import TASK_COMPLETION_THRESHOLD

logger = logging.getLogger(__name__)

class TaskManager:
    """Gère les tâches, assignations et file d'attente"""
    
    def __init__(self, influx_service, kafka_service):
        self.assigned_tasks = {}  # {machine_id: Task}
        self.task_queue = []  # [Task, Task, ...]
        self.interrupted_tasks = []  # Historique des interruptions
        
        self.influx_service = influx_service
        self.kafka_service = kafka_service
        
        self.stats = {
            'total': 0,
            'completed': 0,
            'interrupted': 0,
            'in_queue': 0
        }
    
    def add_task(self, task_data):
        """Ajoute une nouvelle tâche"""
        task = Task.from_kafka_message(task_data)
        self.task_queue.append(task)
        self.stats['total'] += 1
        self.stats['in_queue'] = len(self.task_queue)
        
        logger.info(f"📦 Nouvelle tâche: {task.task_id} ({task.product_name})")
        
        # Écrire taille file d'attente
        self.influx_service.write_queue_size(len(self.task_queue))
    
    def assign_task(self, machine_id):
        """Assigne la première tâche de la file à une machine"""
        if not self.task_queue:
            return None
        
        # Prendre la tâche haute priorité en premier
        high_priority = [t for t in self.task_queue if t.priority == 'HAUTE']
        if high_priority:
            task = high_priority[0]
            self.task_queue.remove(task)
        else:
            task = self.task_queue.pop(0)
        
        # Assigner
        task.assign(machine_id)
        self.assigned_tasks[machine_id] = task
        
        logger.info(f"✅ Assignation: {task.task_id} → {machine_id} ({task.duration_minutes} min)")
        
        # Publier dans Kafka
        assignment = {
            'task_id': task.task_id,
            'machine_id': machine_id,
            'product_name': task.product_name,
            'quantity': task.quantity,
            'duration_minutes': task.duration_minutes,
            'timestamp': datetime.now().isoformat()
        }
        self.kafka_service.publish_assignment(assignment)
        
        # Écrire dans InfluxDB
        self.influx_service.write_task_assignment(
            machine_id,
            task.task_id,
            task.product_name,
            task.duration_minutes
        )
        
        self.stats['in_queue'] = len(self.task_queue)
        self.influx_service.write_queue_size(len(self.task_queue))
        
        return task
    
    def check_completed_tasks(self):
        completed = []
        
        for machine_id, task in list(self.assigned_tasks.items()):
            if task.is_completed():
                completed.append(machine_id)
                logger.info(f"✅ Tâche terminée: {task.task_id} sur {machine_id}")
                self.stats['completed'] += 1
                
                # ✅ ÉCRIRE LE STATUT DISPONIBLE DANS INFLUXDB
                self.influx_service.write_machine_status(
                    machine_id,
                    "DISPONIBLE",
                    f"Tâche {task.task_id} terminée"
                )
        
        for machine_id in completed:
            self.assigned_tasks.pop(machine_id)
        
        return completed
        
    def handle_machine_failure(self, machine_id):
        """Gère la panne d'une machine avec tâche en cours"""
        if machine_id not in self.assigned_tasks:
            return
        
        task = self.assigned_tasks[machine_id]
        progress = task.get_progress()
        
        if progress < TASK_COMPLETION_THRESHOLD:
            logger.warning(f"⚠️  PANNE {machine_id} - Tâche {task.task_id} à {progress*100:.0f}%")
            logger.warning(f"   🔄 Réassignation avec priorité HAUTE")
            
            # Remettre en file avec haute priorité
            task.priority = 'HAUTE'
            task.machine_id = None
            task.start_time = None
            self.task_queue.insert(0, task)
            
            # Historique
            self.interrupted_tasks.append({
                'task_id': task.task_id,
                'machine_id': machine_id,
                'progress': progress,
                'timestamp': datetime.now()
            })
            
            self.stats['interrupted'] += 1
            self.stats['in_queue'] = len(self.task_queue)
        else:
            logger.info(f"✅ Tâche {task.task_id} à {progress*100:.0f}% → Considérée terminée")
            self.stats['completed'] += 1
        
        self.assigned_tasks.pop(machine_id)
    
    def get_assigned_task(self, machine_id):
        """Récupère la tâche assignée à une machine"""
        return self.assigned_tasks.get(machine_id)
    
    def get_queue_size(self):
        """Taille de la file d'attente"""
        return len(self.task_queue)
    
    def get_stats(self):
        """Retourne les statistiques"""
        self.stats['in_queue'] = len(self.task_queue)
        return self.stats