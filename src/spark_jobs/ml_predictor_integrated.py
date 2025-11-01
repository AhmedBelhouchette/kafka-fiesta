"""
Prédicteur ML intégré - Utilise le modèle RandomForest entraîné
Entraîne le modèle au démarrage puis fait les prédictions en streaming
"""

from kafka import KafkaConsumer, KafkaProducer
import json
import time
from datetime import datetime
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
import pickle
import os
import uuid

# Configuration
KAFKA_BOOTSTRAP = 'localhost:9092'
TOPIC_INPUT = 'donnees-capteurs'
TOPIC_OUTPUT = 'alertes-maintenance'

class MLMaintenancePredictor:
    """Prédicteur de maintenance utilisant RandomForest"""
    
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.feature_buffer = {}  # Buffer pour les features temporelles (1min, 5min, 10min)
        self.is_trained = False
        
    def load_or_train_model(self):
        """Charge le modèle s'il existe, sinon l'entraîne"""
        model_path = "models/rf_model.pkl"
        scaler_path = "models/scaler.pkl"
        
        if os.path.exists(model_path) and os.path.exists(scaler_path):
            print("Chargement du modele existant...")
            with open(model_path, 'rb') as f:
                self.model = pickle.load(f)
            with open(scaler_path, 'rb') as f:
                self.scaler = pickle.load(f)
            self.is_trained = True
            print("Modele charge avec succes!")
        else:
            print("Entrainement du modele...")
            self.train_model()
    
    def train_model(self):
        """Entraîne le modèle RandomForest sur les données historiques"""
        # Charger les données d'entraînement
        training_data_path = "data/training_data.parquet"
        
        if not os.path.exists(training_data_path):
            print(f"ERREUR: Fichier de training introuvable: {training_data_path}")
            print("ATTENTION: Generez d'abord les donnees avec: python src/spark_jobs/generate_training_data.py")
            return False
        
        print(f"Chargement des donnees depuis {training_data_path}")
        df = pd.read_parquet(training_data_path)
        
        print(f"Donnees chargees: {len(df)} lignes")
        print(f"Distribution des classes:")
        print(df['type_panne'].value_counts())
        
        # Préparer les features
        feature_cols = [
            'vibration', 'temperature', 'pression',
            'consommation_electrique', 'charge_travail',
            'vibration_moyenne_1min', 'temperature_moyenne_1min',
            'pression_moyenne_1min', 'vibration_ecart_type_1min',
            'pression_ecart_type_1min', 'temperature_max_1min'
        ]
        
        X = df[feature_cols].fillna(0)
        
        # Encoder les labels
        label_mapping = {
            'aucune': 0,
            'pause_requise': 1,
            'maintenance_requise': 2
        }
        y = df['type_panne'].map(label_mapping)
        
        # Normaliser les features
        X_scaled = self.scaler.fit_transform(X)
        
        # Entraîner RandomForest
        print("Entrainement du RandomForest...")
        self.model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )
        self.model.fit(X_scaled, y)
        
        # Évaluer
        score = self.model.score(X_scaled, y)
        print(f"Precision du modele: {score:.4f}")
        
        # Sauvegarder
        os.makedirs("models", exist_ok=True)
        with open("models/rf_model.pkl", 'wb') as f:
            pickle.dump(self.model, f)
        with open("models/scaler.pkl", 'wb') as f:
            pickle.dump(self.scaler, f)
        
        print("Modele sauvegarde dans models/")
        self.is_trained = True
        return True
    
    def compute_features(self, data, machine_id):
        """
        Calcule les features temporelles pour une machine
        En utilisant un buffer des dernières mesures (1min, 5min, 10min)
        """
        # Initialiser le buffer pour cette machine si nécessaire
        if machine_id not in self.feature_buffer:
            self.feature_buffer[machine_id] = []
        
        # Ajouter la mesure actuelle
        self.feature_buffer[machine_id].append({
            'timestamp': datetime.now(),
            'vibration': data['vibration'],
            'temperature': data['temperature'],
            'pression': data['pression']
        })
        
        # Garder seulement les 10 dernières minutes (600 secondes)
        cutoff_time = datetime.now().timestamp() - 600
        self.feature_buffer[machine_id] = [
            m for m in self.feature_buffer[machine_id]
            if m['timestamp'].timestamp() > cutoff_time
        ]
        
        buffer = self.feature_buffer[machine_id]
        now = datetime.now().timestamp()
        
        # Calculer les statistiques pour différentes fenêtres temporelles
        if len(buffer) > 1:
            # Buffer 1 minute
            buffer_1min = [m for m in buffer if now - m['timestamp'].timestamp() <= 60]
            # Buffer 5 minutes
            buffer_5min = [m for m in buffer if now - m['timestamp'].timestamp() <= 300]
            # Buffer 10 minutes
            buffer_10min = buffer
            
            vibrations_1min = [m['vibration'] for m in buffer_1min] if buffer_1min else [data['vibration']]
            temperatures_5min = [m['temperature'] for m in buffer_5min] if buffer_5min else [data['temperature']]
            pressions_10min = [m['pression'] for m in buffer_10min]
            
            features = {
                'vibration': data['vibration'],
                'temperature': data['temperature'],
                'pression': data['pression'],
                'consommation_electrique': data['consommation_electrique'],
                'charge_travail': data['charge_travail'],
                'vibration_moyenne_1min': np.mean(vibrations_1min),
                'temperature_moyenne_1min': np.mean([m['temperature'] for m in buffer_1min]) if buffer_1min else data['temperature'],
                'pression_moyenne_1min': np.mean([m['pression'] for m in buffer_1min]) if buffer_1min else data['pression'],
                'vibration_ecart_type_1min': np.std(vibrations_1min) if len(vibrations_1min) > 1 else 0.0,
                'pression_ecart_type_1min': np.std([m['pression'] for m in buffer_1min]) if len(buffer_1min) > 1 else 0.0,
                'temperature_max_1min': np.max([m['temperature'] for m in buffer_1min]) if buffer_1min else data['temperature'],
                # Features supplémentaires pour l'output
                'temperature_moyenne_5min': np.mean(temperatures_5min),
                'pression_ecart_type_10min': np.std(pressions_10min) if len(pressions_10min) > 1 else 0.0
            }
        else:
            # Pas assez de données, utiliser les valeurs actuelles
            features = {
                'vibration': data['vibration'],
                'temperature': data['temperature'],
                'pression': data['pression'],
                'consommation_electrique': data['consommation_electrique'],
                'charge_travail': data['charge_travail'],
                'vibration_moyenne_1min': data['vibration'],
                'temperature_moyenne_1min': data['temperature'],
                'pression_moyenne_1min': data['pression'],
                'vibration_ecart_type_1min': 0.0,
                'pression_ecart_type_1min': 0.0,
                'temperature_max_1min': data['temperature'],
                'temperature_moyenne_5min': data['temperature'],
                'pression_ecart_type_10min': 0.0
            }
        
        return features
    
    def predict(self, data, machine_id):
        """
        Prédit le type de panne avec le modèle ML
        
        Returns:
            (type_panne, prediction_code, probability, features_dict)
        """
        if not self.is_trained:
            return ('aucune', 0, 1.0, {})
        
        # Calculer les features
        features = self.compute_features(data, machine_id)
        
        # Créer le vecteur de features
        feature_vector = np.array([[
            features['vibration'],
            features['temperature'],
            features['pression'],
            features['consommation_electrique'],
            features['charge_travail'],
            features['vibration_moyenne_1min'],
            features['temperature_moyenne_1min'],
            features['pression_moyenne_1min'],
            features['vibration_ecart_type_1min'],
            features['pression_ecart_type_1min'],
            features['temperature_max_1min']
        ]])
        
        # Normaliser
        feature_vector_scaled = self.scaler.transform(feature_vector)
        
        # Prédire
        prediction = self.model.predict(feature_vector_scaled)[0]
        probabilities = self.model.predict_proba(feature_vector_scaled)[0]
        
        # Mapper la prédiction
        type_mapping = {
            0: 'aucune',
            1: 'pause_requise',
            2: 'maintenance_requise'
        }
        
        type_panne = type_mapping[prediction]
        max_prob = np.max(probabilities)
        
        # Retourner aussi les features ML calculées
        features_ml = {
            'vibration_moyenne_1min': float(features['vibration_moyenne_1min']),
            'temperature_moyenne_5min': float(features['temperature_moyenne_5min']),
            'pression_ecart_type_10min': float(features['pression_ecart_type_10min'])
        }
        
        return (type_panne, int(prediction), float(max_prob), features_ml)
    
    def create_alert(self, data, type_panne, prediction, probability, features_ml):
        """
        Crée un message d'alerte conforme au nouveau schéma
        
        Schema:
        {
          "alert_id": "UUID",
          "machine_id": "string",
          "timestamp_detection": "ISO 8601",
          "type_panne": "enum: 'aucune', 'pause_requise', 'maintenance_requise'",
          "probabilite_panne": "float (0.0-1.0)",
          "metriques_actuelles": {...},
          "action_recommandee": "enum: 'CONTINUER', 'PAUSE', 'ARRETER'",
          "features_ml": {...}
        }
        """
        # Déterminer l'action recommandée
        action_map = {
            0: "CONTINUER",
            1: "PAUSE",
            2: "ARRETER"
        }
        
        alert = {
            "alert_id": str(uuid.uuid4()),
            "machine_id": data['machine_id'],
            "timestamp_detection": datetime.now().isoformat(),
            "type_panne": type_panne,
            "probabilite_panne": round(probability, 4),
            "metriques_actuelles": {
                "vibration": round(data['vibration'], 2),
                "temperature": round(data['temperature'], 2),
                "pression": round(data['pression'], 2),
                "consommation_electrique": round(data['consommation_electrique'], 2),
                "charge_travail": round(data['charge_travail'], 2)
            },
            "action_recommandee": action_map[prediction],
            "features_ml": {
                "vibration_moyenne_1min": round(features_ml['vibration_moyenne_1min'], 2),
                "temperature_moyenne_5min": round(features_ml['temperature_moyenne_5min'], 2),
                "pression_ecart_type_10min": round(features_ml['pression_ecart_type_10min'], 2)
            }
        }
        return alert
    
    def run_streaming(self):
        """Lance le système de prédiction en streaming"""
        print("="*70)
        print("SYSTEME DE PREDICTION ML EN STREAMING")
        print("="*70)
        print(f"Lecture depuis: {TOPIC_INPUT}")
        print(f"Alertes vers: {TOPIC_OUTPUT}")
        print(f"Modele: RandomForest (100 arbres)")
        print(f"Filtrage: UNIQUEMENT pause_requise et maintenance_requise")
        print("="*70)
        print()
        
        # Créer consumer et producer
        consumer = KafkaConsumer(
            TOPIC_INPUT,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='ml-maintenance-predictor-group'
        )
        
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        print("Connexion etablie a Kafka")
        print("En attente de donnees...\n")
        
        message_count = 0
        alert_count = 0
        
        try:
            for message in consumer:
                data = message.value
                message_count += 1
                
                # Prédiction avec le modèle ML
                type_panne, prediction, probability, features_ml = self.predict(data, data['machine_id'])
                
                # Log de toutes les prédictions
                status = "[OK]" if prediction == 0 else ("[WARN]" if prediction == 1 else "[CRIT]")
                conf_str = f"({probability*100:.1f}%)"
                print(f"{status} {data['machine_id']:15} | V:{data['vibration']:5.1f} T:{data['temperature']:5.1f} P:{data['pression']:6.1f} | {type_panne:20} {conf_str}")
                
                # Envoyer alerte SEULEMENT si panne détectée
                if prediction > 0:  # 1 = pause_requise, 2 = maintenance_requise
                    alert = self.create_alert(data, type_panne, prediction, probability, features_ml)
                    producer.send(TOPIC_OUTPUT, value=alert)
                    alert_count += 1
                    
                    action_label = "[PAUSE]" if prediction == 1 else "[STOP]"
                    print(f"  {action_label} -> ALERTE ML: {alert['action_recommandee']} (prob: {probability*100:.1f}%)")
                
                # Statistiques toutes les 50 messages
                if message_count % 50 == 0:
                    alert_rate = alert_count/message_count*100
                    print(f"\nStatistiques ML: {message_count} messages | {alert_count} alertes ({alert_rate:.1f}%)\n")
                
        except KeyboardInterrupt:
            print("\n\nArret du systeme...")
            print(f"Total: {message_count} messages traites | {alert_count} alertes envoyees")
            consumer.close()
            producer.close()
            print("Termine proprement")


def main():
    """Point d'entrée principal"""
    predictor = MLMaintenancePredictor()
    
    # Charger ou entraîner le modèle
    predictor.load_or_train_model()
    
    if not predictor.is_trained:
        print("ERREUR: Impossible de continuer sans modele entraine")
        return
    
    # Lancer le streaming
    predictor.run_streaming()


if __name__ == "__main__":
    main()
