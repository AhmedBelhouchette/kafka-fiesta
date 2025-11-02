"""
Gestionnaire des alertes ML avec timeout et retour d'urgence
Auteur: Ahmed Belhouchette (@AhmedBelhouchette10)
Date: 2025-11-02
"""

from datetime import datetime
import logging
from models.alerte_ml import AlerteML
from models.machine_state import MachineState, MIN_AVAILABLE_MACHINES
from influxdb_client import Point

logger = logging.getLogger(__name__)

class AlertManager:
    """Gère les alertes ML et leur cycle de vie"""
    
    def __init__(self, influx_service):
        self.ml_alerts = {}  # {machine_id: AlerteML}
        self.influx_service = influx_service
        self.stats = {
            'pauses_expired': 0,
            'emergency_returns': 0,
            'repairs': 0
        }
    
    def add_alert(self, machine_id, action, severity="MOYENNE", details=""):
        """Ajoute ou met à jour une alerte ML"""
        alert = AlerteML(
            machine_id=machine_id,
            action=action,
            timestamp=datetime.now(),
            severity=severity,
            details=details
        )
        
        self.ml_alerts[machine_id] = alert
        
        emoji = "🟡" if action == "PAUSE" else "🔴"
        logger.info(f"{emoji} Alerte ML: {machine_id} → {action}")
        
        # Écrire dans InfluxDB
        status = MachineState.PAUSE if action == "PAUSE" else MachineState.ARRET
        self.influx_service.write_machine_status(machine_id, status, details)
        
        # 🆕 ENREGISTRER L'HEURE DE DÉBUT DE PAUSE
        if action == "PAUSE":
            try:
                point = Point("pause_starts") \
                    .tag("machine_id", machine_id) \
                    .field("pause_start_timestamp", datetime.now().timestamp()) \
                    .field("pause_start_iso", datetime.now().isoformat())
                
                if self.influx_service.write_api:
                    self.influx_service.write_api.write(
                        bucket=self.influx_service.bucket,
                        record=point
                    )
                    logger.debug(f"📝 Heure de pause enregistrée pour {machine_id}")
            except Exception as e:
                logger.error(f"❌ Erreur enregistrement pause start: {e}")
    
    def check_expired_pauses(self):
        """Vérifie et supprime les pauses expirées"""
        expired = []
        
        for machine_id, alert in list(self.ml_alerts.items()):
            if alert.is_expired():
                expired.append(machine_id)
                logger.info(f"✅ Pause expirée: {machine_id} → Retour en service")
                self.influx_service.write_machine_status(
                    machine_id, 
                    MachineState.DISPONIBLE, 
                    "Pause expirée - Retour automatique"
                )
                self.stats['pauses_expired'] += 1
        
        for machine_id in expired:
            self.ml_alerts.pop(machine_id)
        
        return len(expired) > 0
    
    def check_emergency_return(self, available_count, task_queue_size):
        """
        Retour anticipé des machines en PAUSE si urgence
        Urgence = tâches en attente ET pas assez de machines disponibles
        """
        if task_queue_size == 0:
            return False
        
        if available_count >= MIN_AVAILABLE_MACHINES:
            return False
        
        # Chercher une machine en PAUSE
        paused_machines = [
            machine_id for machine_id, alert in self.ml_alerts.items()
            if alert.action == "PAUSE"
        ]
        
        if not paused_machines:
            return False
        
        # Retour anticipé de la première machine en pause
        machine_id = paused_machines[0]
        time_remaining = self.ml_alerts[machine_id].time_remaining()
        
        logger.warning(f"🚨 URGENCE: Retour anticipé de {machine_id} (restait {time_remaining}s)")
        self.ml_alerts.pop(machine_id)
        
        self.influx_service.write_machine_status(
            machine_id,
            MachineState.DISPONIBLE,
            f"Retour urgent - {task_queue_size} tâches en attente"
        )
        
        self.stats['emergency_returns'] += 1
        return True
    
    def repair_machine(self, machine_id):
        """Répare une machine (supprime l'alerte)"""
        if machine_id not in self.ml_alerts:
            logger.warning(f"⚠️  Machine {machine_id} n'est pas en alerte")
            return False
        
        alert = self.ml_alerts.pop(machine_id)
        logger.info(f"🔧 Machine {machine_id} RÉPARÉE (était en {alert.action})")
        
        self.influx_service.write_machine_status(
            machine_id,
            MachineState.DISPONIBLE,
            "Réparation manuelle terminée"
        )
        
        self.stats['repairs'] += 1
        return True
    
    def is_machine_blocked(self, machine_id):
        """Vérifie si une machine est bloquée par une alerte"""
        return machine_id in self.ml_alerts
    
    def get_alert(self, machine_id):
        """Récupère l'alerte d'une machine"""
        return self.ml_alerts.get(machine_id)
    
    def get_all_alerts(self):
        """Retourne toutes les alertes actives"""
        return {mid: alert.to_dict() for mid, alert in self.ml_alerts.items()}
    
    def get_stats(self):
        """Retourne les statistiques"""
        return self.stats
    
    def get_time_remaining(self, machine_id):
        """Retourne le temps restant pour une machine en pause"""
        if machine_id not in self.ml_alerts:
            return None
        
        alert = self.ml_alerts[machine_id]
        if alert.action != "PAUSE":
            return None
        
        return alert.time_remaining()