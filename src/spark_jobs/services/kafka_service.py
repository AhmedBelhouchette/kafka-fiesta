"""
Service pour gérer les interactions avec Kafka
Auteur: Ahmed Belhouchette (@AhmedBelhouchette10)
Date: 2025-11-02
"""

from kafka import KafkaProducer, KafkaConsumer
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class KafkaService:
    """Gestion des opérations Kafka - Version Optimisée"""
    
    def __init__(self, bootstrap_servers):
        """
        Initialise le service Kafka
        
        Args:
            bootstrap_servers (str): Adresse des brokers Kafka (ex: "kafka:29092")
        """
        self.bootstrap_servers = bootstrap_servers
        
        # Producer Kafka - Configuration optimisée
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Garantit que le message est bien reçu
            retries=3,   # Réessaie en cas d'échec
            max_in_flight_requests_per_connection=1  # Garantit l'ordre des messages
        )
        
        logger.info(f"✅ Kafka Producer connecté: {bootstrap_servers}")
    
    def create_consumer(self, topics, group_id):
        """
        Crée un consumer Kafka pour lire des messages
        
        Args:
            topics (list): Liste des topics à écouter
            group_id (str): Identifiant du groupe de consommateurs
        
        Returns:
            KafkaConsumer: Consumer configuré
        """
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',  # ✅ LIT UNIQUEMENT LES NOUVEAUX MESSAGES
            enable_auto_commit=True,     # ✅ COMMIT AUTOMATIQUE DES OFFSETS
            group_id=group_id
        )
        
        logger.info(f"✅ Kafka Consumer créé: {topics} (group: {group_id})")
        return consumer
    
    def publish_assignment(self, assignment):
        """
        Publie une assignation de tâche vers le topic 'assignations-taches'
        
        Args:
            assignment (dict): Données de l'assignation
                - task_id: ID de la tâche
                - machine_id: ID de la machine assignée
                - timestamp: Horodatage
        """
        try:
            self.producer.send('assignations-taches', value=assignment)
            self.producer.flush()  # Force l'envoi immédiat
            
            logger.info(
                f"📤 Assignation publiée: {assignment['task_id']} → {assignment['machine_id']}"
            )
        
        except Exception as e:
            logger.error(f"❌ Erreur publication assignation: {e}")
    
    def publish_status(self, machine_id, status, details=""):
        """
        Publie un changement de statut machine vers le topic 'statuts-machines'
        
        Args:
            machine_id (str): ID de la machine
            status (str): Nouveau statut (DISPONIBLE, ASSIGNEE, PAUSE, ARRET)
            details (str): Détails supplémentaires
        """
        try:
            message = {
                'machine_id': machine_id,
                'status': status,
                'details': details,
                'timestamp': datetime.now().isoformat()
            }
            
            self.producer.send('statuts-machines', value=message)
            self.producer.flush()
            
            logger.info(f"📤 Statut publié: {machine_id} → {status}")
        
        except Exception as e:
            logger.error(f"❌ Erreur publication statut: {e}")
    
    def publish_alert(self, alert_data):
        """
        Publie une alerte vers le topic 'alertes-systeme'
        
        Args:
            alert_data (dict): Données de l'alerte
        """
        try:
            alert_data['timestamp'] = datetime.now().isoformat()
            self.producer.send('alertes-systeme', value=alert_data)
            self.producer.flush()
            
            logger.info(f"📤 Alerte publiée: {alert_data.get('machine_id')}")
        
        except Exception as e:
            logger.error(f"❌ Erreur publication alerte: {e}")
    
    def flush(self):
        """Force l'envoi de tous les messages en attente"""
        try:
            self.producer.flush()
            logger.debug("✅ Messages Kafka envoyés")
        except Exception as e:
            logger.error(f"❌ Erreur flush Kafka: {e}")
    
    def close(self):
        """Ferme proprement le producer Kafka"""
        try:
            self.producer.flush()
            self.producer.close()
            logger.info("✅ Kafka Producer fermé proprement")
        except Exception as e:
            logger.error(f"❌ Erreur fermeture Kafka: {e}")