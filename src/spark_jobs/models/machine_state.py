"""
Constantes et énumérations pour les états des machines
"""
import os

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

# Configuration — env-overridable so a "demo timing" mode can make the full
# machine lifecycle (pause / recover / idle) visible within a short session.
PAUSE_TIMEOUT_SECONDS = int(os.getenv("PAUSE_TIMEOUT_SECONDS", "600"))   # default 10 min
IDLE_SHUTDOWN_SECONDS = int(os.getenv("IDLE_SHUTDOWN_SECONDS", "1800"))  # default 30 min
MIN_AVAILABLE_MACHINES = 1
TASK_COMPLETION_THRESHOLD = 0.80  # 80% = tâche considérée terminée