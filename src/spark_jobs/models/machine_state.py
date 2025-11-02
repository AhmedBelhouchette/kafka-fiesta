"""
Constantes et énumérations pour les états des machines
"""

class MachineState:
    """États possibles d'une machine"""
    DISPONIBLE = "DISPONIBLE"
    ASSIGNEE = "ASSIGNEE"
    PAUSE = "PAUSE"
    ARRET = "ARRET"
    IDLE = "IDLE"  # Arrêt économie d'énergie

class AlertAction:
    """Actions recommandées par le ML"""
    CONTINUER = "CONTINUER"
    PAUSE = "PAUSE"
    ARRETER = "ARRETER"

class AlertSeverity:
    """Niveaux de sévérité des alertes"""
    BASSE = "BASSE"
    MOYENNE = "MOYENNE"
    HAUTE = "HAUTE"

# Configuration
PAUSE_TIMEOUT_SECONDS = 600  # 10 minutes
IDLE_SHUTDOWN_SECONDS = 1800  # 30 minutes
MIN_AVAILABLE_MACHINES = 1
TASK_COMPLETION_THRESHOLD = 0.80  # 80% = tâche considérée terminée