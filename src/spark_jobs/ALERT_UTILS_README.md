# Utilitaires d'Alerte - Documentation

## Vue d'ensemble

Le module `alert_utils.py` fournit des fonctions pour créer, valider et gérer les alertes de maintenance au format JSON standardisé.

## Structure JSON des Alertes

```json
{
  "alert_id": "string (UUID)",
  "machine_id": "string",
  "timestamp_detection": "string (ISO 8601)",
  "type_panne": "string (enum: 'aucune', 'pause_requise', 'maintenance_requise')",
  "probabilite_panne": "float (0.0-1.0)",
  "metriques_actuelles": {
    "vibration": "float",
    "temperature": "float",
    "pression": "float",
    "consommation_electrique": "float",
    "charge_travail": "float"
  },
  "action_recommandee": "string (enum: 'CONTINUER', 'PAUSE', 'ARRETER')",
  "features_ml": {
    "vibration_moyenne_1min": "float",
    "temperature_moyenne_5min": "float",
    "pression_ecart_type_10min": "float"
  }
}
```

## Fonctions Principales

### `create_alert_json()`

Crée une alerte JSON complète avec tous les champs requis.

**Paramètres :**
- `machine_id` : Identifiant de la machine (ex: "POMPE-01")
- `type_panne` : Type de panne ('aucune', 'pause_requise', 'maintenance_requise')
- `probabilite_panne` : Probabilité de panne (0.0 à 1.0)
- `metriques_actuelles` : Dictionnaire des métriques actuelles
- `features_ml` : Dictionnaire des features ML calculées
- `action_recommandee` : Action recommandée ('CONTINUER', 'PAUSE', 'ARRETER')

**Exemple :**
```python
from alert_utils import create_alert_json

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
        'temperature_moyenne_5min': 88.9,
        'pression_ecart_type_10min': 12.4
    },
    action_recommandee="ARRETER"
)
```

### `validate_alert_json()`

Valide qu'une alerte contient tous les champs requis et les valeurs correctes.

**Exemple :**
```python
from alert_utils import validate_alert_json

is_valid = validate_alert_json(alert)
if not is_valid:
    print("Alerte invalide !")
```

### `classify_prediction()`

Classifie une probabilité de panne en type de panne.

**Exemple :**
```python
from alert_utils import classify_prediction

type_panne = classify_prediction(0.75)  # Retourne: 'pause_requise'
```

**Règles de classification :**
- `probabilite < 0.5` → `aucune`
- `0.5 ≤ probabilite < 0.8` → `pause_requise`
- `probabilite ≥ 0.8` → `maintenance_requise`

### `get_action_from_type_panne()`

Détermine l'action recommandée en fonction du type de panne.

**Exemple :**
```python
from alert_utils import get_action_from_type_panne

action = get_action_from_type_panne('maintenance_requise')  # Retourne: 'ARRETER'
```

**Mapping des actions :**
- `aucune` → `CONTINUER`
- `pause_requise` → `PAUSE`
- `maintenance_requise` → `ARRETER`

## Utilisation dans un Job Spark

```python
from pyspark.sql import DataFrame
from alert_utils import create_alert_json, classify_prediction, get_action_from_type_panne
import json

def process_predictions(df: DataFrame):
    """
    Traite les prédictions et crée les alertes JSON
    """
    for row in df.collect():
        # Classifier la prédiction
        type_panne = classify_prediction(row.probabilite_panne)
        
        # Déterminer l'action
        action = get_action_from_type_panne(type_panne)
        
        # Créer l'alerte
        alert = create_alert_json(
            machine_id=row.machine_id,
            type_panne=type_panne,
            probabilite_panne=row.probabilite_panne,
            metriques_actuelles={
                'vibration': row.vibration,
                'temperature': row.temperature,
                'pression': row.pression,
                'consommation_electrique': row.consommation_electrique,
                'charge_travail': row.charge_travail
            },
            features_ml={
                'vibration_moyenne_1min': row.vibration_moyenne_1min,
                'temperature_moyenne_5min': row.temperature_moyenne_5min,
                'pression_ecart_type_10min': row.pression_ecart_type_10min
            },
            action_recommandee=action
        )
        
        # Envoyer à Kafka
        send_to_kafka('alertes-maintenance', json.dumps(alert))
```

## Tests

Pour tester le module :

```bash
python src/spark_jobs/alert_utils.py
```

Cela affichera un exemple d'alerte et exécutera les tests de validation.
