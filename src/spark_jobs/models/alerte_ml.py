"""
Modèle pour les alertes ML avec gestion du timeout
"""

from datetime import datetime
from .machine_state import PAUSE_TIMEOUT_SECONDS

class AlerteML:
    """Représente une alerte ML avec timeout automatique"""
    
    def __init__(self, machine_id, action, timestamp, severity="MOYENNE", details=""):
        self.machine_id = machine_id
        self.action = action  # PAUSE ou ARRETER
        self.timestamp = timestamp
        self.severity = severity
        self.details = details
    
    def is_expired(self):
        """Vérifie si la pause est expirée (seulement pour PAUSE)"""
        if self.action != "PAUSE":
            return False
        
        elapsed = (datetime.now() - self.timestamp).total_seconds()
        return elapsed > PAUSE_TIMEOUT_SECONDS
    
    def time_remaining(self):
        """Retourne le temps restant en secondes"""
        if self.action != "PAUSE":
            return 0
        
        elapsed = (datetime.now() - self.timestamp).total_seconds()
        remaining = max(0, PAUSE_TIMEOUT_SECONDS - elapsed)
        return int(remaining)
    
    def to_dict(self):
        """Conversion en dictionnaire"""
        return {
            'machine_id': self.machine_id,
            'action': self.action,
            'timestamp': self.timestamp.isoformat(),
            'severity': self.severity,
            'details': self.details,
            'time_remaining': self.time_remaining()
        }
    
    def __repr__(self):
        return f"AlerteML({self.machine_id}, {self.action}, {self.severity})"
    