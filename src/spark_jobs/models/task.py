"""
Modèle pour les tâches de production
"""

from datetime import datetime

class Task:
    """Représente une tâche de production"""
    
    def __init__(self, task_id, product_name, quantity, duration_minutes, priority='NORMALE'):
        self.task_id = task_id
        self.product_name = product_name
        self.quantity = quantity
        self.duration_minutes = duration_minutes
        self.priority = priority
        self.start_time = None
        self.machine_id = None
        self.partial_completion = 0.0  # Pour tâches interrompues
    
    def assign(self, machine_id):
        """Assigne la tâche à une machine"""
        self.machine_id = machine_id
        self.start_time = datetime.now()
    
    def get_progress(self):
        """Calcule le progrès de la tâche (0.0 à 1.0)"""
        if not self.start_time:
            return 0.0
        
        elapsed = (datetime.now() - self.start_time).total_seconds()
        duration = self.duration_minutes * 60
        progress = min(1.0, elapsed / duration)
        return progress + self.partial_completion
    
    def is_completed(self):
        """Vérifie si la tâche est terminée"""
        if not self.start_time:
            return False
        
        elapsed = (datetime.now() - self.start_time).total_seconds()
        return elapsed >= (self.duration_minutes * 60)
    
    def time_remaining(self):
        """Temps restant en minutes"""
        if not self.start_time:
            return self.duration_minutes
        
        elapsed = (datetime.now() - self.start_time).total_seconds() / 60
        return max(0, self.duration_minutes - elapsed)
    
    def to_dict(self):
        """Conversion en dictionnaire"""
        return {
            'task_id': self.task_id,
            'product_name': self.product_name,
            'quantity': self.quantity,
            'duration_minutes': self.duration_minutes,
            'priority': self.priority,
            'machine_id': self.machine_id,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'progress': self.get_progress(),
            'time_remaining': self.time_remaining()
        }
    
    @classmethod
    def from_kafka_message(cls, message):
        """Crée une Task depuis un message Kafka"""
        # Support plusieurs formats de messages
        task_id = message.get('task_id') or message.get('id') or message.get('tache_id')
        product_name = message.get('product_name') or message.get('produit') or message.get('product')
        quantity = message.get('quantity') or message.get('quantite') or message.get('qty', 0)
        duration = message.get('duration_minutes') or message.get('duree_minutes') or message.get('duration', 60)
        
        if not task_id:
            raise ValueError(f"Message Kafka invalide - pas de task_id: {message}")
        
        return cls(
            task_id=task_id,
            product_name=product_name,
            quantity=int(quantity),
            duration_minutes=int(duration)
        )
    def __repr__(self):
        return f"Task({self.task_id}, {self.product_name}, {self.duration_minutes}min)"