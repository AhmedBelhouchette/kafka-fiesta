# 🎉 Système de Sauvegarde HDFS - Complet !

## ✅ Ce qui a été réalisé

### 1. **Module HDFS Data Saver créé** (`src/spark_jobs/hdfs_data_saver.py`)

Un nouveau composant complet qui:
- ✅ **Capture** les données des capteurs depuis Kafka (`donnees-capteurs`)
- ✅ **Enrichit** les données avec les prédictions ML depuis `alertes-maintenance`
- ✅ **Combine** les données capteurs + prédictions + features ML
- ✅ **Sauvegarde** dans HDFS au format Parquet partitionné par date

### 2. **Architecture du flux de données**

```
┌──────────────────┐
│  Simulateur      │  Génère données capteurs
│  Capteurs        │  (vibration, température, etc.)
└────────┬─────────┘
         │
         v
┌──────────────────┐
│ donnees-capteurs │  Topic Kafka
└────────┬─────────┘
         │
         v
┌──────────────────┐     ┌──────────────────┐
│  ML Predictor    │────>│alertes-maintenance│  Topic Kafka
│  Integrated      │     │  (Prédictions ML) │
└──────────────────┘     └────────┬──────────┘
         │                         │
         └──────────┬──────────────┘
                    │
                    v
          ┌──────────────────┐
          │  HDFS Data Saver │
          │                  │
          │  - Capture       │
          │  - Enrichit      │
          │  - Sauvegarde    │
          └────────┬─────────┘
                   │
                   v
          ┌──────────────────┐
          │  HDFS            │
          │  /data/          │
          │  predictions/    │
          │  maintenance/    │
          │  date=2025-11-01/│
          │  ├─ data_xxx.parquet
          │  ├─ data_yyy.parquet
          │  └─ data_zzz.parquet
          └──────────────────┘
```

### 3. **Données enrichies sauvegardées**

Chaque enregistrement contient:

**Données capteurs originales:**
- `machine_id`, `timestamp`
- `vibration`, `temperature`, `pression`
- `consommation_electrique`, `charge_travail`

**Prédictions ML ajoutées:**
- `alert_id` (UUID unique)
- `timestamp_detection`
- `type_panne` (aucune / pause_requise / maintenance_requise)
- `probabilite_panne` (0.0 à 1.0)
- `action_recommandee` (CONTINUER / PAUSE / ARRETER)

**Features ML temporelles:**
- `vibration_moyenne_1min`
- `temperature_moyenne_5min`
- `pression_ecart_type_10min`

**Métadonnées:**
- `timestamp_saved` (quand sauvegardé dans HDFS)

### 4. **Fonctionnalités clés**

✅ **Sauvegarde par batch**
- Accumule 50 enregistrements avant de sauvegarder
- Ou sauvegarde toutes les 30 secondes (configurable)

✅ **Partitionnement par date**
- Structure: `/data/predictions/maintenance/date=YYYY-MM-DD/`
- Facilite les requêtes par période

✅ **Format Parquet**
- Compression efficace (~70% vs JSON)
- Schéma typé
- Compatible Spark/Pandas

✅ **Cache des alertes**
- Maintient une correspondance machine_id → alerte
- Enrichit automatiquement les données capteurs

✅ **Gestion d'erreurs robuste**
- Retry automatique en cas d'échec
- Les données restent en buffer si échec
- Logs détaillés de chaque opération

### 5. **Documentation complète**

✅ **HDFS_SAVER_GUIDE.md**
- Instructions détaillées d'utilisation
- Exemples de code pour lire les données
- Dépannage et troubleshooting

✅ **test_hdfs_saver.py**
- Script de vérification de l'infrastructure
- Vérifie Docker, Kafka, HDFS, modèles ML
- Affiche le contenu de HDFS

### 6. **Correction Windows**

✅ **Problème résolu**: Les chemins Windows causaient des erreurs HDFS
✅ **Solution**: Copie en 2 étapes (Windows → Docker → HDFS)
✅ **Résultat**: Compatible multi-plateforme

## 📊 Exemple d'enregistrement sauvegardé

```json
{
  "machine_id": "POMPE-01",
  "timestamp": "2025-11-01T19:59:23.123456",
  "vibration": 1.2,
  "temperature": 65.5,
  "pression": 145.3,
  "consommation_electrique": 18.5,
  "charge_travail": 85.2,
  "alert_id": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "timestamp_detection": "2025-11-01T19:59:23.456789",
  "type_panne": "pause_requise",
  "probabilite_panne": 0.53,
  "action_recommandee": "PAUSE",
  "vibration_moyenne_1min": 1.15,
  "temperature_moyenne_5min": 62.3,
  "pression_ecart_type_10min": 3.2,
  "timestamp_saved": "2025-11-01T19:59:30.000000"
}
```

## 🚀 Comment l'utiliser

### Étape 1: Vérifier l'infrastructure
```powershell
python src/spark_jobs/test_hdfs_saver.py
```

### Étape 2: Lancer les 3 composants (3 terminaux)

**Terminal 1 - Simulateur:**
```powershell
python src/simulators/simulateur_capteurs.py
```

**Terminal 2 - ML Predictor:**
```powershell
python src/spark_jobs/ml_predictor_integrated.py
```

**Terminal 3 - HDFS Data Saver:**
```powershell
python src/spark_jobs/hdfs_data_saver.py
```

### Étape 3: Vérifier les données sauvegardées
```powershell
# Voir le contenu HDFS
python src/spark_jobs/test_hdfs_saver.py --show-content

# Ou directement
docker exec hadoop-master hdfs dfs -ls -R /data/predictions/maintenance
```

## 📈 Statistiques attendues

Avec le simulateur générant ~270 messages/minute (9 machines × 0.5Hz × 60s):
- **Taux d'alertes**: 2-3% (réaliste industriel)
- **Sauvegardes**: ~5-6 par minute (50 messages/batch)
- **Taille fichier**: ~5-10 KB par batch (50 enregistrements)
- **Stockage/heure**: ~2-3 MB

## 🎯 Cas d'usage

### 1. Analyse historique
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("hdfs://hadoop-master:9000/data/predictions/maintenance")

# Machines avec le plus de pannes
df.filter(df.type_panne != "aucune") \
  .groupBy("machine_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show()
```

### 2. Calcul de métriques
```python
# Taux d'alertes par machine
df.groupBy("machine_id", "type_panne").count().show()

# Distribution des probabilités
df.filter(df.probabilite_panne > 0).describe("probabilite_panne").show()
```

### 3. Export pour ML
```python
# Exporter les features pour réentraînement
features_df = df.select(
    "vibration", "temperature", "pression",
    "vibration_moyenne_1min", "temperature_moyenne_5min",
    "pression_ecart_type_10min",
    "type_panne"
)
features_df.write.parquet("/data/training/new_data.parquet")
```

## ✨ Avantages du système

1. **📦 Centralisation**: Toutes les données au même endroit
2. **🔍 Traçabilité**: UUID unique pour chaque alerte
3. **⏱️ Temporalité**: Features ML avec fenêtres temporelles
4. **📊 Analytics-ready**: Format Parquet optimisé pour Spark
5. **🗂️ Partitionné**: Requêtes efficaces par date
6. **🛡️ Fiable**: Gestion d'erreurs et retry automatique

## 🎓 Prochaines étapes possibles

1. **Dashboard temps réel**: Visualiser les données avec Grafana
2. **Alertes avancées**: Déclencher des actions sur seuils
3. **ML amélioré**: Réentraîner le modèle avec les données historiques
4. **Archivage**: Compresser les données > 30 jours
5. **API REST**: Exposer les données via une API

## 📚 Documentation complète

- `HDFS_SAVER_GUIDE.md` - Guide d'utilisation détaillé
- `SCHEMAS.md` - Schémas des données
- `PROJECT_STRUCTURE.md` - Architecture du projet

---

**🎉 Le système est maintenant complet et opérationnel !**

Vous avez un pipeline end-to-end:
1. Simulation de données industrielles réalistes
2. Prédiction ML temps réel (2-3% d'alertes)
3. Sauvegarde enrichie dans HDFS
4. Prêt pour l'analyse et la visualisation !
