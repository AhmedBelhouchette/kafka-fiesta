"""
Gestionnaire des machines et de leur disponibilité
✅ LIT LES ÉTATS DEPUIS KAFKA (Single Source of Truth)
"""

from datetime import datetime
import logging
from models.machine_state import MachineState, IDLE_SHUTDOWN_SECONDS
from kafka import KafkaConsumer
import json

logger = logging.getLogger(__name__)

class MachineManager:
    """Gère l'état et la disponibilité des machines"""
    
    def __init__(self, machines, influx_service):
        self.machines = machines
        self.influx_service = influx_service
        
        # Suivi activité
        self.last_activity = {m: datetime.now() for m in machines}
        self.idle_machines = set()
        
        logger.info(f"🔧 Machines: {', '.join(machines)}")
    
    def get_machine_states_from_kafka(self):
        """✅ LIT LES ÉTATS DES MACHINES DEPUIS KAFKA (Single Source of Truth)"""
        machine_states = {}
        
        try:
            consumer = KafkaConsumer(
                'statuts-machines',
                bootstrap_servers='kafka:29092',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                consumer_timeout_ms=2000,
                group_id='resource-manager-state-reader',
                enable_auto_commit=False
            )
            
            # Lire les derniers états
            for message in consumer:
                data = message.value
                machine_id = data.get('machine_id')
                status = data.get('status')
                machine_states[machine_id] = status
            
            consumer.close()
        
        except Exception as e:
            logger.warning(f"⚠️  Erreur lecture états Kafka: {e}")
        
        return machine_states
    
    def get_available_machines(self, assigned_tasks, task_queue_size):
        """
        ✅ RETOURNE LES MACHINES DISPONIBLES (lit depuis Kafka)
        Gère le réveil des machines en IDLE si besoin
        """
        # Lire les états depuis Kafka
        kafka_states = self.get_machine_states_from_kafka()
        
        available = []
        
        for machine_id in self.machines:
            # Machine occupée
            if machine_id in assigned_tasks:
                continue
            
            # Vérifier l'état depuis Kafka
            status = kafka_states.get(machine_id, 'DISPONIBLE')
            
            # Machine en IDLE → réveiller si travail
            if status == 'IDLE':
                if task_queue_size > 0:
                    self.wake_machine(machine_id)
                    available.append(machine_id)
                continue
            
            # Machine disponible
            if status == 'DISPONIBLE':
                available.append(machine_id)
        
        return available
    
    def wake_machine(self, machine_id):
        """Réveille une machine en IDLE"""
        if machine_id in self.idle_machines:
            self.idle_machines.remove(machine_id)
        
        logger.info(f"⚡ Réveil {machine_id} (travail disponible)")
        self.influx_service.write_machine_status(
            machine_id,
            MachineState.DISPONIBLE,
            "Réveil automatique"
        )
        self.last_activity[machine_id] = datetime.now()
    
    def check_idle_machines(self, assigned_tasks, task_queue_size):
        """
        ✅ ARRÊTE LES MACHINES INACTIVES (lit depuis Kafka)
        SEULEMENT s'il n'y a pas de travail en attente
        """
        if task_queue_size > 0:
            return  # Pas d'arrêt si travail en attente
        
        # Lire les états depuis Kafka
        kafka_states = self.get_machine_states_from_kafka()
        
        now = datetime.now()
        newly_idle = []
        
        for machine_id in self.machines:
            # Vérifier l'état depuis Kafka
            status = kafka_states.get(machine_id, 'DISPONIBLE')
            
            # Déjà occupée, en alerte, ou en idle
            if machine_id in assigned_tasks or status != 'DISPONIBLE':
                continue
            
            # Calculer temps inactivité
            idle_time = (now - self.last_activity[machine_id]).total_seconds()
            
            if idle_time > IDLE_SHUTDOWN_SECONDS:
                self.idle_machines.add(machine_id)
                newly_idle.append(machine_id)
                logger.info(f"💤 Arrêt économie: {machine_id} (inactif {idle_time/60:.0f} min)")
                self.influx_service.write_machine_status(
                    machine_id,
                    MachineState.IDLE,
                    f"Économie énergie - inactif {idle_time/60:.0f} min"
                )
        
        return newly_idle
    
    def update_activity(self, machine_id):
        """Met à jour le timestamp d'activité"""
        self.last_activity[machine_id] = datetime.now()
    
    def select_best_machine(self, available_machines):
        """
        Sélectionne la meilleure machine parmi celles disponibles
        Critères: température, charge de travail
        """
        if not available_machines:
            return None
        
        best_machine = None
        best_score = float('inf')
        
        for machine_id in available_machines:
            state = self.influx_service.get_machine_state(machine_id)
            
            if not state:
                continue
            
            temp = state.get('temperature', 70)
            charge = state.get('charge_travail', 50)
            
            # Score: plus c'est bas, mieux c'est
            score = (temp / 100) * 0.6 + (charge / 100) * 0.4
            
            if score < best_score:
                best_score = score
                best_machine = machine_id
        
        return best_machine or available_machines[0]
    
    def get_machine_count_by_state(self, assigned_tasks):
        """✅ COMPTE LES MACHINES PAR ÉTAT (lit depuis Kafka)"""
        states = {
            'disponible': 0,
            'assignee': 0,
            'pause': 0,
            'arret': 0,
            'idle': 0
        }
        
        # Lire les états depuis Kafka
        kafka_states = self.get_machine_states_from_kafka()
        
        for machine_id in self.machines:
            if machine_id in assigned_tasks:
                states['assignee'] += 1
            else:
                status = kafka_states.get(machine_id, 'DISPONIBLE')
                
                if status == 'DISPONIBLE':
                    states['disponible'] += 1
                elif status == 'PAUSE':
                    states['pause'] += 1
                elif status == 'ARRET':
                    states['arret'] += 1
                elif status == 'IDLE':
                    states['idle'] += 1
        
        return states