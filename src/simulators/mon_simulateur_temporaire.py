import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
CAPTEURS_TOPIC = "donnees-capteurs"

# --- Initialisation du Producteur Kafka ---
# value_serializer s'assure que les messages sont bien encodés en JSON
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Simulateur temporaire démarré. Envoi de données vers le topic 'donnees-capteurs'...")
print("Appuyez sur Ctrl+C pour arrêter.")

# --- Boucle principale pour envoyer des données ---
try:
    while True:
        # Choisir une machine au hasard
        machine_id = f"POMPE-{random.randint(1, 5)}"
        
        # Créer un message avec des données aléatoires mais réalistes
        message = {
            "machine_id": machine_id,
            "timestamp": datetime.utcnow().isoformat() + "Z", # Format ISO 8601
            "etat": random.choice(["en_marche", "arrete"]),
            "charge_travail": round(random.uniform(0, 100), 2) if random.random() > 0.1 else 0.0,
            "consommation_electrique": round(random.uniform(5, 25), 2),
            "vibration": round(random.uniform(0.5, 5.0), 3),
            "temperature": round(random.uniform(40, 95), 2),
            "pression": round(random.uniform(100, 200), 2)
        }

        # Envoyer le message au topic Kafka
        producer.send(CAPTEURS_TOPIC, value=message)
        
        print(f"Envoyé: {message}")

        # Attendre 1 seconde avant d'envoyer le prochain message
        time.sleep(1)

except KeyboardInterrupt:
    print("\nSimulateur arrêté.")
finally:
    # S'assurer que tous les messages en attente sont envoyés avant de quitter
    producer.flush()
    producer.close()