"""
Utilitaires pour la création et la gestion des alertes de maintenance
"""

import uuid
from datetime import datetime
from typing import Dict, Any


def create_alert_json(
    machine_id: str,
    type_panne: str,
    probabilite_panne: float,
    metriques_actuelles: Dict[str, float],
    features_ml: Dict[str, float],
    action_recommandee: str
) -> Dict[str, Any]:
    """
    Crée une alerte JSON au format attendu par le Resource Manager
    
    Args:
        machine_id: Identifiant de la machine (ex: "POMPE-01")
        type_panne: Type de panne ('aucune', 'pause_requise', 'maintenance_requise')
        probabilite_panne: Probabilité de panne (0.0 à 1.0)
        metriques_actuelles: Dictionnaire des métriques actuelles
            {
                'vibration': float,
                'temperature': float,
                'pression': float,
                'consommation_electrique': float,
                'charge_travail': float
            }
        features_ml: Dictionnaire des features calculées pour le ML
            {
                'vibration_moyenne_1min': float,
                'temperature_moyenne_5min': float,
                'pression_ecart_type_10min': float,
                ...
            }
        action_recommandee: Action recommandée ('CONTINUER', 'PAUSE', 'ARRETER')
    
    Returns:
        Dictionnaire représentant l'alerte au format JSON
    """
    return {
        "alert_id": str(uuid.uuid4()),
        "machine_id": machine_id,
        "timestamp_detection": datetime.utcnow().isoformat() + "Z",
        "type_panne": type_panne,
        "probabilite_panne": round(probabilite_panne, 4),
        "metriques_actuelles": {
            "vibration": round(metriques_actuelles.get('vibration', 0.0), 2),
            "temperature": round(metriques_actuelles.get('temperature', 0.0), 2),
            "pression": round(metriques_actuelles.get('pression', 0.0), 2),
            "consommation_electrique": round(metriques_actuelles.get('consommation_electrique', 0.0), 2),
            "charge_travail": round(metriques_actuelles.get('charge_travail', 0.0), 2)
        },
        "action_recommandee": action_recommandee,
        "features_ml": {
            k: round(v, 4) for k, v in features_ml.items()
        }
    }


def validate_alert_json(alert: Dict[str, Any]) -> bool:
    """
    Valide qu'une alerte contient tous les champs requis
    
    Args:
        alert: Dictionnaire représentant l'alerte
    
    Returns:
        True si l'alerte est valide, False sinon
    """
    required_fields = [
        'alert_id', 'machine_id', 'timestamp_detection',
        'type_panne', 'probabilite_panne', 'metriques_actuelles',
        'action_recommandee', 'features_ml'
    ]
    
    # Vérifier les champs principaux
    for field in required_fields:
        if field not in alert:
            return False
    
    # Vérifier les métriques actuelles
    required_metriques = ['vibration', 'temperature', 'pression', 
                          'consommation_electrique', 'charge_travail']
    for metrique in required_metriques:
        if metrique not in alert['metriques_actuelles']:
            return False
    
    # Vérifier les valeurs enum
    valid_type_panne = ['aucune', 'pause_requise', 'maintenance_requise']
    if alert['type_panne'] not in valid_type_panne:
        return False
    
    valid_actions = ['CONTINUER', 'PAUSE', 'ARRETER']
    if alert['action_recommandee'] not in valid_actions:
        return False
    
    # Vérifier la probabilité
    if not (0.0 <= alert['probabilite_panne'] <= 1.0):
        return False
    
    return True


def get_action_from_type_panne(type_panne: str) -> str:
    """
    Détermine l'action recommandée en fonction du type de panne
    
    Args:
        type_panne: Type de panne ('aucune', 'pause_requise', 'maintenance_requise')
    
    Returns:
        Action recommandée ('CONTINUER', 'PAUSE', 'ARRETER')
    """
    action_mapping = {
        'aucune': 'CONTINUER',
        'pause_requise': 'PAUSE',
        'maintenance_requise': 'ARRETER'
    }
    return action_mapping.get(type_panne, 'CONTINUER')


def classify_prediction(probabilite_panne: float, threshold_pause: float = 0.5, 
                        threshold_maintenance: float = 0.8) -> str:
    """
    Classifie une prédiction en fonction de la probabilité de panne
    
    Args:
        probabilite_panne: Probabilité de panne (0.0 à 1.0)
        threshold_pause: Seuil pour 'pause_requise'
        threshold_maintenance: Seuil pour 'maintenance_requise'
    
    Returns:
        Type de panne ('aucune', 'pause_requise', 'maintenance_requise')
    """
    if probabilite_panne >= threshold_maintenance:
        return 'maintenance_requise'
    elif probabilite_panne >= threshold_pause:
        return 'pause_requise'
    else:
        return 'aucune'


# Exemple d'utilisation
if __name__ == "__main__":
    import json
    
    # Créer une alerte exemple
    alert = create_alert_json(
        machine_id="POMPE-01",
        type_panne="maintenance_requise",
        probabilite_panne=0.94,
        metriques_actuelles={
            'vibration': 3.8,
            'temperature': 89.3,
            'pression': 135.2,
            'consommation_electrique': 25.7,
            'charge_travail': 98.5
        },
        features_ml={
            'vibration_moyenne_1min': 3.75,
            'vibration_moyenne_5min': 3.65,
            'temperature_moyenne_5min': 88.9,
            'temperature_max_1h': 91.2,
            'pression_ecart_type_10min': 12.4
        },
        action_recommandee="ARRETER"
    )
    
    # Afficher l'alerte
    print("Exemple d'alerte JSON:")
    print(json.dumps(alert, indent=2, ensure_ascii=False))
    
    # Valider l'alerte
    print(f"\nAlerte valide: {validate_alert_json(alert)}")
    
    # Tester la classification
    print("\nTests de classification:")
    print(f"Probabilité 0.3 -> {classify_prediction(0.3)}")
    print(f"Probabilité 0.6 -> {classify_prediction(0.6)}")
    print(f"Probabilité 0.9 -> {classify_prediction(0.9)}")
