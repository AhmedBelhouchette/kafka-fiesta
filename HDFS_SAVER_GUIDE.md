# Guide d'utilisation - HDFS Data Saver

## Vue d'ensemble

Le module `hdfs_data_saver.py` capture les données des capteurs depuis Kafka, les enrichit avec les prédictions ML, et les sauvegarde dans HDFS au format Parquet.

## Architecture du flux de données

```
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────┐
│  Simulateur     │─────>│ donnees-capteurs │─────>│                 │
│  Capteurs       │      │   (Kafka Topic)  │      │                 │
└─────────────────┘      └──────────────────┘      │                 │
                                                     │  HDFS Data      │
┌─────────────────┐      ┌──────────────────┐      │  Saver          │
│  ML Predictor   │─────>│alertes-maintenance│─────>│                 │
│  Integrated     │      │   (Kafka Topic)  │      │  - Enrichit     │
└─────────────────┘      └──────────────────┘      │  - Sauvegarde   │
                                                     └────────┬────────┘
                                                              │
                                                              v
                                                     ┌─────────────────┐
                                                     │  HDFS           │
                                                     │  /data/         │
                                                     │  predictions/   │
                                                     │  maintenance/   │
                                                     └─────────────────┘
```

## Fonctionnalités

### 1. **Capture des données**
- Lit les données des capteurs depuis `donnees-capteurs`
- Lit les alertes ML depuis `alertes-maintenance`

### 2. **Enrichissement**
Chaque enregistrement capteur est enrichi avec:
- **Données originales du capteur:**
  - `machine_id`, `timestamp`
  - `vibration`, `temperature`, `pression`
  - `consommation_electrique`, `charge_travail`

- **Informations de prédiction ML:**
  - `alert_id` (UUID de l'alerte)
  - `timestamp_detection` (quand l'alerte a été générée)
  - `type_panne` (`aucune`, `pause_requise`, `maintenance_requise`)
  - `probabilite_panne` (0.0 à 1.0)
  - `action_recommandee` (`CONTINUER`, `PAUSE`, `ARRETER`)

- **Features ML temporelles:**
  - `vibration_moyenne_1min`
  - `temperature_moyenne_5min`
  - `pression_ecart_type_10min`

- **Métadonnées:**
  - `timestamp_saved` (timestamp de sauvegarde dans HDFS)

### 3. **Sauvegarde dans HDFS**
- Format: **Parquet** (compression efficace, schéma typé)
- Partitionnement: **Par date** (`date=YYYY-MM-DD`)
- Chemin: `/data/predictions/maintenance/date=YYYY-MM-DD/data_YYYYMMDD_HHMMSS.parquet`

### 4. **Stratégie de sauvegarde**
Le système sauvegarde automatiquement quand:
- **Batch size atteint**: 50 enregistrements (configurable)
- **OU Intervalle de temps écoulé**: 30 secondes (configurable)

## Utilisation

### Étape 1: Démarrer l'infrastructure
```powershell
# Démarrer Docker Compose
docker-compose up -d

# Configurer HDFS et Kafka
python src/spark_jobs/setup_infrastructure.py
```

### Étape 2: Lancer les composants dans l'ordre

**Terminal 1 - Simulateur de capteurs:**
```powershell
python src/simulators/simulateur_capteurs.py
```

**Terminal 2 - Prédicteur ML:**
```powershell
python src/spark_jobs/ml_predictor_integrated.py
```

**Terminal 3 - HDFS Data Saver:**
```powershell
python src/spark_jobs/hdfs_data_saver.py
```

**Terminal 4 (Optionnel) - Resource Manager:**
```powershell
python src/services/resource_manager_consumer.py
```

### Étape 3: Vérifier les données dans HDFS

**Lister les fichiers:**
```powershell
docker exec hadoop-master hdfs dfs -ls -R /data/predictions/maintenance
```

**Voir le contenu d'un fichier:**
```powershell
docker exec hadoop-master hdfs dfs -cat /data/predictions/maintenance/date=2025-11-01/data_*.parquet | head
```

**Vérifier la taille:**
```powershell
docker exec hadoop-master hdfs dfs -du -h /data/predictions/maintenance
```

## Configuration

Vous pouvez modifier les paramètres dans `hdfs_data_saver.py`:

```python
# Dans la fonction main()
BATCH_SIZE = 50        # Nombre d'enregistrements avant sauvegarde
SAVE_INTERVAL = 30     # Secondes avant sauvegarde forcée
```

## Structure des données sauvegardées

### Schéma Parquet

| Colonne | Type | Description |
|---------|------|-------------|
| `machine_id` | string | Identifiant de la machine |
| `timestamp` | string | Timestamp de capture du capteur |
| `vibration` | float | Vibration (mm/s) |
| `temperature` | float | Température (°C) |
| `pression` | float | Pression (bar) |
| `consommation_electrique` | float | Consommation (kW) |
| `charge_travail` | float | Charge de travail (%) |
| `alert_id` | string | UUID de l'alerte (null si aucune) |
| `timestamp_detection` | string | Timestamp de détection d'alerte |
| `type_panne` | string | Type de panne prédite |
| `probabilite_panne` | float | Probabilité de panne (0.0-1.0) |
| `action_recommandee` | string | Action recommandée |
| `vibration_moyenne_1min` | float | Feature ML - moyenne 1min |
| `temperature_moyenne_5min` | float | Feature ML - moyenne 5min |
| `pression_ecart_type_10min` | float | Feature ML - écart-type 10min |
| `timestamp_saved` | string | Timestamp de sauvegarde HDFS |

### Exemple d'enregistrement

```json
{
  "machine_id": "POMPE-01",
  "timestamp": "2025-11-01T19:45:23.123456",
  "vibration": 1.2,
  "temperature": 65.5,
  "pression": 145.3,
  "consommation_electrique": 18.5,
  "charge_travail": 85.2,
  "alert_id": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "timestamp_detection": "2025-11-01T19:45:23.456789",
  "type_panne": "pause_requise",
  "probabilite_panne": 0.53,
  "action_recommandee": "PAUSE",
  "vibration_moyenne_1min": 1.15,
  "temperature_moyenne_5min": 62.3,
  "pression_ecart_type_10min": 3.2,
  "timestamp_saved": "2025-11-01T19:45:30.000000"
}
```

## Analyse des données

### Lecture depuis Python/PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Analyse Maintenance") \
    .getOrCreate()

# Lire toutes les données
df = spark.read.parquet("hdfs://hadoop-master:9000/data/predictions/maintenance")

# Filtrer une date spécifique
df_today = spark.read.parquet("hdfs://hadoop-master:9000/data/predictions/maintenance/date=2025-11-01")

# Analyse des alertes
df.groupBy("type_panne").count().show()

# Machines avec le plus d'alertes
df.filter(df.type_panne != "aucune") \
  .groupBy("machine_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show()
```

### Lecture depuis Pandas (via PyArrow)

```python
import pandas as pd
import subprocess
import tempfile
import os

def read_hdfs_parquet(hdfs_path):
    """Télécharge et lit un fichier Parquet depuis HDFS"""
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        temp_path = tmp.name
    
    # Télécharger depuis HDFS
    subprocess.run([
        "docker", "exec", "hadoop-master",
        "hdfs", "dfs", "-get", hdfs_path, temp_path
    ])
    
    # Lire avec Pandas
    df = pd.read_parquet(temp_path)
    os.remove(temp_path)
    return df

# Utilisation
df = read_hdfs_parquet("/data/predictions/maintenance/date=2025-11-01/data_20251101_194530.parquet")
print(df.describe())
print(df['type_panne'].value_counts())
```

## Monitoring

### Logs du système
Le système affiche:
- `[CACHE]` : Réception d'une alerte (mise en cache)
- `[DATA]` : Réception de données capteur normales
- `[ALERT-PAUSE]` : Données avec alerte de pause
- `[ALERT-ARRETER]` : Données avec alerte d'arrêt
- `[OK]` : Sauvegarde réussie
- `[ERREUR]` : Problème de sauvegarde
- `[STATS]` : Statistiques après sauvegarde

### Statistiques affichées
```
======================================================================
SAUVEGARDE HDFS - 50 enregistrements
======================================================================
[OK] Fichier temporaire cree: /tmp/xyz.parquet (50 lignes)
[UPLOAD] Copie vers HDFS: /data/predictions/maintenance/date=2025-11-01/data_20251101_194530.parquet
[SUCCESS] Donnees sauvegardees dans HDFS: ...
[INFO] Taille: 5.2 K
[OK] 50 enregistrements sauvegardes
[STATS] Alertes: 1/50 (2.0%)
======================================================================
```

## Dépannage

### Problème: "No such container: hadoop-master"
**Solution:** Vérifier que Hadoop est démarré:
```powershell
docker ps | findstr hadoop
docker-compose up -d
```

### Problème: "Permission denied" dans HDFS
**Solution:** Vérifier les permissions HDFS:
```powershell
docker exec hadoop-master hdfs dfs -chmod -R 777 /data
```

### Problème: Buffer ne se vide pas
**Solution:** 
- Réduire `BATCH_SIZE` (ex: 10 au lieu de 50)
- Réduire `SAVE_INTERVAL` (ex: 10 secondes au lieu de 30)

### Problème: Alertes ne sont pas enrichies
**Solution:** 
- Vérifier que `ml_predictor_integrated.py` est lancé
- Vérifier que les alertes arrivent sur le topic `alertes-maintenance`

## Performances

### Taille des fichiers
- **Format Parquet** : Compression ~70% par rapport au JSON
- **50 enregistrements** : ~5-10 KB
- **1000 enregistrements** : ~100-200 KB

### Débit
- Peut gérer **plusieurs centaines de messages/seconde**
- Limité principalement par la vitesse d'écriture HDFS

## Prochaines étapes

1. **Analyse avancée** : Créer des jobs Spark pour analyser les données historiques
2. **Dashboard** : Visualiser les tendances de pannes par machine
3. **Archivage** : Compresser les données anciennes (> 30 jours)
4. **Alerting** : Déclencher des alertes si taux de panne > seuil

## Support

Pour plus d'informations, voir:
- `SCHEMAS.md` - Schémas des données
- `PROJECT_STRUCTURE.md` - Architecture du projet
- `SPECIFICATIONS.md` - Spécifications techniques
