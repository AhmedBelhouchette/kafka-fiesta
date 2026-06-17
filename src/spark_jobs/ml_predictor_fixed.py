"""
ML Predictor - Version Corrigée et Compatible
Compatible avec le Resource Manager d'Ahmed
Auteur: Ahmed Belhouchette (@AhmedBelhouchette10)
Date: 2025-11-02
"""

from kafka import KafkaConsumer, KafkaProducer
import json
import time
from datetime import datetime
import pandas as pd
import numpy as np
import pickle
import os
import uuid

# ✅ CONFIGURATION CORRIGÉE
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:29092')
TOPIC_INPUT = 'donnees-capteurs'
TOPIC_OUTPUT = 'alertes-ml'

class MLMaintenancePredictor:
    """Prédicteur de maintenance - Version Compatible"""

    def __init__(self):
        self.model = None
        self.scaler = None
        self.feature_buffer = {}
        self.is_trained = False
        # 🆕 SUIVI DES ÉTATS PRÉCÉDENTS
        self.previous_states = {}

    def load_model(self):
        """Charge le modèle pré-entraîné"""
        model_path = "models/rf_model.pkl"
        scaler_path = "models/scaler.pkl"

        if not os.path.exists(model_path) or not os.path.exists(scaler_path):
            print("⚠️  ATTENTION: Modèles introuvables!")
            print("   Attendu: models/rf_model.pkl et models/scaler.pkl")
            print("   Le système fonctionnera en mode règles simples")
            return False

        print("📦 Chargement du modèle RandomForest...")
        with open(model_path, 'rb') as f:
            self.model = pickle.load(f)
        with open(scaler_path, 'rb') as f:
            self.scaler = pickle.load(f)

        self.is_trained = True
        print("✅ Modèle chargé avec succès!")
        return True

    def compute_features(self, data, machine_id):
        """Compute the 12-feature vector. MUST match scripts/train_model.py."""
        buf = self.feature_buffer.setdefault(machine_id, [])
        buf.append({
            'vibration': data['vibration'],
            'temperature': data['temperature'],
            'pression': data['pression'],
        })
        if len(buf) > 60:
            del buf[:-60]

        win = buf[-12:]  # ~1 min window, matches WINDOW in train_model.py
        vib = np.array([m['vibration'] for m in win], dtype=float)
        temp = np.array([m['temperature'] for m in win], dtype=float)
        pres = np.array([m['pression'] for m in win], dtype=float)

        def slope(s):
            return float(np.polyfit(np.arange(len(s)), s, 1)[0]) if len(s) > 1 else 0.0

        return [
            data['vibration'], data['temperature'], data['pression'],
            data['consommation_electrique'], data['charge_travail'],
            float(vib.mean()), float(temp.mean()),
            float(vib.std()) if len(vib) > 1 else 0.0,
            float(pres.std()) if len(pres) > 1 else 0.0,
            slope(temp), slope(vib), float(temp.max()),
        ]

    def predict(self, data, machine_id):
        """Prédit le type de panne"""
        if not self.is_trained:
            # 🆕 MODE FALLBACK : RÈGLES SIMPLES SI PAS DE MODÈLE
            temp = data['temperature']
            charge = data['charge_travail']
            
            if temp > 90 or charge > 90:
                return ('maintenance_requise', 2, 0.95)
            elif temp > 75 or charge > 70:
                return ('pause_requise', 1, 0.85)
            else:
                return ('aucune', 0, 0.99)

        features = self.compute_features(data, machine_id)
        feature_vector_scaled = self.scaler.transform(np.array([features]))
        prediction = self.model.predict(feature_vector_scaled)[0]
        probabilities = self.model.predict_proba(feature_vector_scaled)[0]

        type_mapping = {
            0: 'aucune',
            1: 'pause_requise',
            2: 'maintenance_requise'
        }

        return (type_mapping[prediction], int(prediction), float(np.max(probabilities)))

    def create_alert(self, data, type_panne, prediction, probability):
        """
        ✅ ALERTE COMPATIBLE AVEC LE RESOURCE MANAGER D'AHMED
        Format: {"machine_id", "action", "timestamp", "temperature", "charge_travail", "severity"}
        """
        action_map = {
            0: "CONTINUER",
            1: "PAUSE",
            2: "ARRETER"
        }

        alert = {
            "machine_id": data['machine_id'],
            "action": action_map[prediction],
            "timestamp": datetime.now().isoformat(),
            "temperature": round(data['temperature'], 2),
            "charge_travail": round(data['charge_travail'], 2),
            "severity": "HAUTE" if prediction == 2 else "MOYENNE"
        }
        
        return alert

    def run_streaming(self):
        """Lance la prédiction en streaming"""
        print("="*70)
        print("🤖 ML PREDICTOR - STREAMING MODE")
        print("="*70)
        print(f"📥 Input:  {TOPIC_INPUT}")
        print(f"📤 Output: {TOPIC_OUTPUT}")
        print(f"🧠 Model:  {'RandomForest' if self.is_trained else 'Rules-Based (Fallback)'}")
        print("="*70)
        print()

        consumer = KafkaConsumer(
            TOPIC_INPUT,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='ml-predictor-group',
            auto_offset_reset='latest',      # ✅ ONLY ONCE
            enable_auto_commit=True          # ✅ ONLY ONCE
        )

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        print("✅ Connecté à Kafka")
        print("⏳ En attente de données...\n")

        message_count = 0
        alert_count = 0

        try:
            for message in consumer:
                data = message.value
                message_count += 1

                machine_id = data['machine_id']
                type_panne, prediction, probability = self.predict(data, machine_id)

                # 🆕 VÉRIFIER SI L'ÉTAT A CHANGÉ
                previous_state = self.previous_states.get(machine_id, 0)
                
                # Log
                emoji = "🟢" if prediction == 0 else ("🟡" if prediction == 1 else "🔴")
                print(f"{emoji} {machine_id:12} | T:{data['temperature']:5.1f}°C C:{data['charge_travail']:5.1f}% | {type_panne:20} ({probability*100:.0f}%)")

                # 🆕 ENVOYER ALERTE UNIQUEMENT SI CHANGEMENT D'ÉTAT
                if prediction != previous_state:
                    if prediction > 0:  # PAUSE ou ARRETER
                        alert = self.create_alert(data, type_panne, prediction, probability)
                        producer.send(TOPIC_OUTPUT, value=alert)
                        producer.flush()
                        alert_count += 1
                        
                        action_emoji = "🟡" if prediction == 1 else "🔴"
                        action_text = "PAUSE" if prediction == 1 else "ARRÊTER"
                        print(f"   📢 {action_emoji} ALERTE ENVOYÉE: {machine_id} → {action_text}")
                    
                    # Mettre à jour l'état précédent
                    self.previous_states[machine_id] = prediction

                if message_count % 25 == 0:
                    print(f"\n📊 Stats: {message_count} messages | {alert_count} alertes ({alert_count/message_count*100:.1f}%)\n")

        except KeyboardInterrupt:
            print("\n🛑 Arrêt du système...")
            print(f"   Total: {message_count} messages | {alert_count} alertes")
            consumer.close()
            producer.close()


def main():
    predictor = MLMaintenancePredictor()
    predictor.load_model()  # Charge le modèle si disponible, sinon mode fallback
    predictor.run_streaming()


if __name__ == "__main__":
    main()