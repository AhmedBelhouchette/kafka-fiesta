"""
Consumer pour afficher les alertes de maintenance envoyées au Resource Manager
"""

from kafka import KafkaConsumer
import json
from datetime import datetime
import sys

KAFKA_BOOTSTRAP = 'localhost:9092'
TOPIC = 'alertes-maintenance'

def display_alert(alert):
    """Affiche une alerte de manière formatée avec le nouveau schéma"""
    # Déterminer le label selon l'action recommandée
    label_map = {
        'CONTINUER': '[OK]',
        'PAUSE': '[WARN]',
        'ARRETER': '[CRIT]'
    }
    label = label_map.get(alert.get('action_recommandee', 'CONTINUER'), '[WARN]')
    
    print(f"\n{label}{'='*68}")
    print(f"ALERTE MAINTENANCE - {alert['machine_id']}")
    print("="*70)
    print(f"Alert ID: {alert.get('alert_id', 'N/A')}")
    print(f"Timestamp: {alert.get('timestamp_detection', alert.get('timestamp', 'N/A'))}")
    print(f"Type de panne: {alert['type_panne'].upper()}")
    print(f"Probabilite: {alert.get('probabilite_panne', 0)*100:.2f}%")
    print(f"Action recommandee: {alert.get('action_recommandee', alert.get('action', 'N/A')).upper()}")
    
    # Métriques actuelles
    metriques = alert.get('metriques_actuelles', alert.get('metriques', {}))
    print("\nMetriques actuelles:")
    print(f"   - Vibration: {metriques.get('vibration', 0):.2f} mm/s")
    print(f"   - Temperature: {metriques.get('temperature', 0):.2f} C")
    print(f"   - Pression: {metriques.get('pression', 0):.2f} bar")
    print(f"   - Consommation: {metriques.get('consommation_electrique', 0):.2f} kW")
    print(f"   - Charge travail: {metriques.get('charge_travail', 0):.1f}%")
    
    # Features ML si disponibles
    if 'features_ml' in alert:
        features = alert['features_ml']
        print("\nFeatures ML (fenetres temporelles):")
        print(f"   - Vibration moy. (1min): {features.get('vibration_moyenne_1min', 0):.2f} mm/s")
        print(f"   - Temperature moy. (5min): {features.get('temperature_moyenne_5min', 0):.2f} C")
        print(f"   - Pression ecart-type (10min): {features.get('pression_ecart_type_10min', 0):.2f} bar")
    
    print("="*70)


def main():
    """Consumer principal"""
    print("="*70)
    print("RESOURCE MANAGER - RECEPTEUR D'ALERTES")
    print("="*70)
    print(f"Topic: {TOPIC}")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print("="*70)
    print("En attente d'alertes...\n")
    
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='resource-manager-group'
    )
    
    alert_count = 0
    
    try:
        for message in consumer:
            alert = message.value
            alert_count += 1
            
            display_alert(alert)
            
            print(f"\nTotal alertes recues: {alert_count}\n")
            
    except KeyboardInterrupt:
        print(f"\n\nArret du Resource Manager")
        print(f"Total: {alert_count} alertes traitees")
        consumer.close()
        print("Deconnexion propre")


if __name__ == "__main__":
    main()
