"""
Models package - Modèles de données
"""

from .machine_state import MachineState, AlertAction, AlertSeverity
from .machine_state import PAUSE_TIMEOUT_SECONDS, IDLE_SHUTDOWN_SECONDS, MIN_AVAILABLE_MACHINES, TASK_COMPLETION_THRESHOLD
from .alerte_ml import AlerteML
from .task import Task

__all__ = [
    'MachineState',
    'AlertAction',
    'AlertSeverity',
    'AlerteML',
    'Task',
    'PAUSE_TIMEOUT_SECONDS',
    'IDLE_SHUTDOWN_SECONDS',
    'MIN_AVAILABLE_MACHINES',
    'TASK_COMPLETION_THRESHOLD'
]
