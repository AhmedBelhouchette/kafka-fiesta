"""
Configuration centralisée pour les jobs Spark
Ce fichier contient tous les paramètres de connexion pour Kafka, HDFS, InfluxDB
"""

# Configuration Kafka
KAFKA_CONFIG = {
    'bootstrap_servers': 'localhost:9092',
    'topics': {
        'donnees_capteurs': 'donnees-capteurs',
        'alertes_maintenance': 'alertes-maintenance',
        'commandes_machines': 'commandes-machines',
        'plan_de_production': 'plan-de-production'
    },
    'consumer_group': 'maintenance-predictor-group',
    'auto_offset_reset': 'latest'
}

# Configuration HDFS
HDFS_CONFIG = {
    'namenode': 'hdfs://localhost:9870',
    'paths': {
        'raw_data': '/data/raw/donnees-capteurs',
        'processed_features': '/data/processed/features',
        'models': '/data/models/maintenance_predictor',
        'predictions': '/data/predictions/maintenance'
    }
}

# Configuration InfluxDB
INFLUXDB_CONFIG = {
    'url': 'http://localhost:8086',
    'token': '',  # À remplir si nécessaire
    'org': 'maintenance',
    'bucket': 'industrial_metrics',
    'measurements': {
        'etat_machines': 'etat_machines',
        'predictions': 'predictions',
        'anomalies_energie': 'anomalies_energie'
    }
}

# Configuration Spark Streaming
SPARK_STREAMING_CONFIG = {
    'app_name': 'MaintenancePredictor',
    'batch_interval': '10 seconds',  # Intervalle de traitement des micro-batches
    'checkpoint_location': '/tmp/spark-checkpoints/maintenance-predictor',
    'kafka_starting_offsets': 'latest',  # ou 'earliest' pour tout relire
    'trigger_once': False  # True pour traitement batch unique
}

# Configuration du Modèle ML
ML_MODEL_CONFIG = {
    'model_path': '/data/models/maintenance_predictor/random_forest_v1',
    'model_version': 'rf_v1.2.0',
    'features': [
        'vibration', 'temperature', 'pression', 
        'consommation_electrique', 'charge_travail',
        'vibration_moyenne_1min', 'vibration_moyenne_5min',
        'temperature_moyenne_5min', 'temperature_max_1h',
        'pression_ecart_type_10min'
    ],
    'target_column': 'type_panne',
    'classes': {
        0: 'aucune',
        1: 'pause_requise',
        2: 'maintenance_requise'
    },
    'thresholds': {
        'aucune': 0.5,
        'pause_requise': 0.8
    }
}

# Configuration des fenêtres temporelles pour le Feature Engineering
WINDOW_CONFIG = {
    'vibration_1min': '1 minute',
    'vibration_5min': '5 minutes',
    'temperature_5min': '5 minutes',
    'temperature_1h': '1 hour',
    'pression_10min': '10 minutes'
}

# Configuration des alertes (Structure JSON simplifiée)
ALERT_CONFIG = {
    'action_mapping': {
        'aucune': 'CONTINUER',
        'pause_requise': 'PAUSE',
        'maintenance_requise': 'ARRETER'
    }
}

# Structure JSON pour les alertes envoyées au Resource Manager
# {
#   "alert_id": "string (UUID)",
#   "machine_id": "string",
#   "timestamp_detection": "string (ISO 8601)",
#   "type_panne": "string (enum: 'aucune', 'pause_requise', 'maintenance_requise')",
#   "probabilite_panne": "float (0.0-1.0)",
#   "metriques_actuelles": {
#     "vibration": "float",
#     "temperature": "float",
#     "pression": "float",
#     "consommation_electrique": "float",
#     "charge_travail": "float"
#   },
#   "action_recommandee": "string (enum: 'CONTINUER', 'PAUSE', 'ARRETER')",
#   "features_ml": {
#     "vibration_moyenne_1min": "float",
#     "temperature_moyenne_5min": "float",
#     "pression_ecart_type_10min": "float"
#   }
# }

# Configuration du logging
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
}
