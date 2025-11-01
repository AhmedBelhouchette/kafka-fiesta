"""
Script d'injection de données machines dans InfluxDB
Simule l'état des machines en temps réel
"""

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import time
import random

# Configuration InfluxDB
client = InfluxDBClient(
    url="http://localhost:8086",
    token="my-super-secret-token",
    org="mon-usine"
)

write_api = client.write_api(write_options=SYNCHRONOUS)
bucket = "donnees-usine"

machines = ["POMPE-1", "POMPE-2", "POMPE-3", "POMPE-4", "POMPE-5"]
etats_possibles = ["operationnelle", "operationnelle", "operationnelle", "maintenance"]

print("=" * 60)
print("📊 INJECTION DE DONNÉES MACHINES DANS INFLUXDB")
print("=" * 60)
print(f"\nMachines: {', '.join(machines)}")
print("Fréquence: Toutes les 10 secondes")
print("Appuyez sur Ctrl+C pour arrêter\n")

compteur = 0

try:
    while True:
        print(f"\n--- INJECTION #{compteur} - {datetime.now().strftime('%H:%M:%S')} ---")
        
        for machine in machines:
            # Générer des valeurs réalistes
            temperature = round(random.uniform(65.0, 85.0), 1)
            vibration = round(random.uniform(1.5, 3.5), 2)
            charge_travail = round(random.uniform(30.0, 80.0), 1)
            consommation = round(random.uniform(8.0, 15.0), 1)
            etat = random.choice(etats_possibles)
            nombre_taches = random.randint(0, 3)
            
            # Créer le point InfluxDB
            point = Point("etat_machines") \
                .tag("machine_id", machine) \
                .field("temperature", temperature) \
                .field("vibration", vibration) \
                .field("charge_travail", charge_travail) \
                .field("consommation_electrique", consommation) \
                .field("etat", etat) \
                .field("nombre_taches", nombre_taches) \
                .time(datetime.utcnow())
            
            # Écrire dans InfluxDB
            write_api.write(bucket=bucket, record=point)
            
            # Afficher
            emoji = "🟢" if etat == "operationnelle" else "🟡"
            print(f"{emoji} {machine}: temp={temperature}°C, vib={vibration}, charge={charge_travail}%, état={etat}")
        
        compteur += 1
        time.sleep(10)  # Attendre 10 secondes

except KeyboardInterrupt:
    print("\n\n🛑 Arrêt de l'injection")
    print(f"✅ {compteur} cycles d'injection effectués")

except Exception as e:
    print(f"\n❌ Erreur: {e}")
    import traceback
    traceback.print_exc()

finally:
    client.close()