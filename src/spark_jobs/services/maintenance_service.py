"""
Service pour gérer les actions de maintenance (réparations)
"""

from kafka import KafkaConsumer
import json
import logging
import threading

logger = logging.getLogger(__name__)

class MaintenanceService:
    """Écoute les demandes de maintenance et répare les machines"""
    
    def __init__(self, bootstrap_servers, callback, break_callback=None):
        self.bootstrap_servers = bootstrap_servers
        self.callback = callback
        self.break_callback = break_callback
        self.running = False
        self.thread = None
    
    def start(self):
        """Démarre le thread d'écoute"""
        self.running = True
        self.thread = threading.Thread(target=self._listen, daemon=True)
        self.thread.start()
        logger.info("🔧 Service maintenance démarré")
    
    def _listen(self):
        """Écoute le topic actions-maintenance"""
        try:
            consumer = KafkaConsumer(
                'actions-maintenance',
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='maintenance-service'
            )
            
            logger.info("👂 Écoute des actions de maintenance...")
            
            while self.running:
                messages = consumer.poll(timeout_ms=1000)
                
                for topic_partition, records in messages.items():
                    for record in records:
                        self._handle_action(record.value)
        
        except Exception as e:
            logger.error(f"❌ Erreur service maintenance: {e}")
    
    def _handle_action(self, action):
        """Traite une action de maintenance"""
        try:
            action_type = action.get('action')
            machine_id = action.get('machine_id')
            
            if action_type == 'REPARER':
                logger.info(f"🔧 Action reçue: RÉPARER {machine_id}")
                self.callback(machine_id)
            
            elif action_type == 'FORCER_ARRET':
                logger.warning(f"⛔ Action reçue: FORCER ARRÊT {machine_id}")
                if self.break_callback:
                    self.break_callback(machine_id)

            else:
                logger.warning(f"⚠️  Action inconnue: {action_type}")
        
        except Exception as e:
            logger.error(f"❌ Erreur traitement action: {e}")
    
    def stop(self):
        """Arrête le service"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("Service maintenance arrêté")