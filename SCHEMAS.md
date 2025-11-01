# 📋 Schémas de Données - Système de Maintenance Prédictive

## 📥 INPUT SCHEMA (Topic Kafka: `donnees-capteurs`)

Données de streaming générées par le simulateur de capteurs.

```json
{
  "machine_id": "string",
  "timestamp": "string (ISO 8601 avec timezone)",
  "etat": "string (enum: 'en_marche', 'arret', 'maintenance')",
  "charge_travail": "float (0-100)",
  "consommation_electrique": "float (kW)",
  "vibration": "float (mm/s)",
  "temperature": "float (°C)",
  "pression": "float (bar)"
}
```

### Exemple
```json
{
  "machine_id": "POMPE-01",
  "timestamp": "2025-11-01T00:31:18.111119+00:00",
  "etat": "en_marche",
  "charge_travail": 58.35,
  "consommation_electrique": 15.97,
  "vibration": 1.25,
  "temperature": 40.08,
  "pression": 132.74
}
```

---

## 📤 OUTPUT SCHEMA (Topic Kafka: `alertes-maintenance`)

Alertes générées par le modèle ML et envoyées au Resource Manager.

```json
{
  "alert_id": "string (UUID v4)",
  "machine_id": "string",
  "timestamp_detection": "string (ISO 8601)",
  "type_panne": "string (enum: 'aucune', 'pause_requise', 'maintenance_requise')",
  "probabilite_panne": "float (0.0-1.0)",
  "metriques_actuelles": {
    "vibration": "float (mm/s)",
    "temperature": "float (°C)",
    "pression": "float (bar)",
    "consommation_electrique": "float (kW)",
    "charge_travail": "float (0-100)"
  },
  "action_recommandee": "string (enum: 'CONTINUER', 'PAUSE', 'ARRETER')",
  "features_ml": {
    "vibration_moyenne_1min": "float (mm/s)",
    "temperature_moyenne_5min": "float (°C)",
    "pression_ecart_type_10min": "float (bar)"
  }
}
```

### Exemple
```json
{
  "alert_id": "a3d4e5f6-7890-12ab-cdef-1234567890ab",
  "machine_id": "POMPE-02",
  "timestamp_detection": "2025-11-01T18:45:23.456789",
  "type_panne": "pause_requise",
  "probabilite_panne": 0.7832,
  "metriques_actuelles": {
    "vibration": 8.5,
    "temperature": 87.3,
    "pression": 146.8,
    "consommation_electrique": 24.5,
    "charge_travail": 94.2
  },
  "action_recommandee": "PAUSE",
  "features_ml": {
    "vibration_moyenne_1min": 8.2,
    "temperature_moyenne_5min": 85.6,
    "pression_ecart_type_10min": 3.4
  }
}
```

---

## 🔄 Mapping des Valeurs

### Type de Panne → Action Recommandée

| `type_panne` | `prediction_code` | `action_recommandee` | Description |
|-------------|-------------------|---------------------|-------------|
| `aucune` | 0 | `CONTINUER` | Fonctionnement normal, continuer l'opération |
| `pause_requise` | 1 | `PAUSE` | Arrêt temporaire recommandé pour inspection |
| `maintenance_requise` | 2 | `ARRETER` | Arrêt immédiat pour maintenance urgente |

### États des Machines (Input)

| État | Description |
|------|-------------|
| `en_marche` | Machine en fonctionnement normal |
| `arret` | Machine arrêtée |
| `maintenance` | Machine en maintenance |

---

## 🧠 Features ML (Fenêtres Temporelles)

Le modèle utilise des features calculées sur différentes fenêtres temporelles pour améliorer la précision :

| Feature | Fenêtre | Description |
|---------|---------|-------------|
| `vibration_moyenne_1min` | 60 secondes | Moyenne glissante des vibrations |
| `temperature_moyenne_5min` | 300 secondes | Moyenne glissante de la température |
| `pression_ecart_type_10min` | 600 secondes | Écart-type de la pression (variabilité) |
| `vibration_ecart_type_1min` | 60 secondes | Variabilité des vibrations (utilisé en interne) |
| `pression_ecart_type_1min` | 60 secondes | Variabilité de la pression courte période |
| `temperature_max_1min` | 60 secondes | Pic de température récent |

---

## 📊 Amélioration de l'Accuracy

### Changements Principaux

1. **UUID pour les alertes** : Chaque alerte a un identifiant unique pour le tracking
2. **Probabilité explicite** : Le champ `probabilite_panne` indique la confiance du modèle
3. **Features ML exposées** : Les features temporelles sont incluses dans l'output pour l'analyse
4. **Actions standardisées** : Utilisation d'enums clairs (`CONTINUER`, `PAUSE`, `ARRETER`)
5. **Fenêtres temporelles multiples** : 1min, 5min, 10min pour capturer différents patterns

### Avantages

- ✅ **Traçabilité** : Chaque alerte a un UUID unique
- ✅ **Transparence** : Probabilité et features ML visibles
- ✅ **Standardisation** : Schéma cohérent et typé
- ✅ **Analyse rétroactive** : Les features ML permettent l'audit des prédictions
- ✅ **Meilleure accuracy** : Fenêtres temporelles multiples capturent les tendances

---

## 🔧 Utilisation

### Simulateur (Input)
```bash
python src/simulators/simulateur_capteurs.py
```

### Prédicteur ML (Processing)
```bash
python src/spark_jobs/ml_predictor_integrated.py
```

### Resource Manager (Output)
```bash
python src/services/resource_manager_consumer.py
```

---

## 📝 Notes Techniques

- **Encodage** : UTF-8
- **Timestamp format** : ISO 8601
- **Timezone** : UTC recommandé
- **JSON** : Serialization/Deserialization automatique via `kafka-python`
- **Buffer size** : 10 minutes de données pour les fenêtres temporelles
