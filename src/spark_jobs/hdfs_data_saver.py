"""
HDFS Data Saver - Enrichit et sauvegarde les données dans HDFS
Capture les données des capteurs + prédictions ML et les sauvegarde dans HDFS
"""

from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import subprocess
import os
import tempfile
import time

# Configuration
KAFKA_BOOTSTRAP = 'localhost:9092'
TOPIC_SENSORS = 'donnees-capteurs'
TOPIC_ALERTS = 'alertes-maintenance'
HDFS_CONTAINER = 'hadoop-master'
HDFS_BASE_PATH = '/data/predictions/maintenance'


class HDFSDataSaver:
    """Sauvegarde les données enrichies dans HDFS"""
    
    def __init__(self, batch_size=100, save_interval_seconds=60):
        """
        Args:
            batch_size: Nombre de messages à accumuler avant sauvegarde
            save_interval_seconds: Intervalle de temps entre les sauvegardes (secondes)
        """
        self.batch_size = batch_size
        self.save_interval = save_interval_seconds
        self.data_buffer = []
        self.alerts_cache = {}  # Cache des alertes par machine_id
        self.last_save_time = time.time()
        
    def run_hdfs_command(self, command):
        """Exécute une commande HDFS via Docker"""
        try:
            cmd = ["docker", "exec", HDFS_CONTAINER] + command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            return True, result.stdout
        except subprocess.CalledProcessError as e:
            return False, f"Erreur HDFS: {e.stderr}"
    
    def save_to_hdfs(self, df, partition_date):
        """Sauvegarde un DataFrame dans HDFS au format Parquet"""
        if df.empty:
            print("[INFO] Aucune donnee a sauvegarder")
            return False
        
        # Créer un fichier temporaire local
        with tempfile.NamedTemporaryFile(mode='w', suffix='.parquet', delete=False) as tmp_file:
            temp_path = tmp_file.name
        
        try:
            # Sauvegarder en Parquet localement
            df.to_parquet(temp_path, engine='pyarrow', index=False)
            print(f"[OK] Fichier temporaire cree: {temp_path} ({len(df)} lignes)")
            
            # Créer le chemin HDFS avec partition par date
            hdfs_dir = f"{HDFS_BASE_PATH}/date={partition_date}"
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            hdfs_file = f"{hdfs_dir}/data_{timestamp}.parquet"
            
            # Créer le répertoire HDFS si nécessaire
            success, _ = self.run_hdfs_command([
                "hdfs", "dfs", "-mkdir", "-p", hdfs_dir
            ])
            
            if not success:
                print(f"[ERREUR] Impossible de creer le repertoire HDFS: {hdfs_dir}")
                return False
            
            # Copier le fichier dans le container Docker d'abord
            container_temp_path = f"/tmp/data_{timestamp}.parquet"
            copy_cmd = ["docker", "cp", temp_path, f"{HDFS_CONTAINER}:{container_temp_path}"]
            result = subprocess.run(copy_cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"[ERREUR] Echec de la copie vers le container: {result.stderr}")
                return False
            
            # Copier depuis le container vers HDFS
            print(f"[UPLOAD] Copie vers HDFS: {hdfs_file}")
            success, output = self.run_hdfs_command([
                "hdfs", "dfs", "-put", "-f", container_temp_path, hdfs_file
            ])
            
            # Nettoyer le fichier temporaire dans le container
            self.run_hdfs_command(["rm", "-f", container_temp_path])
            
            if success:
                print(f"[SUCCESS] Donnees sauvegardees dans HDFS: {hdfs_file}")
                
                # Vérifier la taille du fichier
                success, size_output = self.run_hdfs_command([
                    "hdfs", "dfs", "-du", "-h", hdfs_file
                ])
                if success:
                    print(f"[INFO] Taille: {size_output.strip()}")
                
                return True
            else:
                print(f"[ERREUR] Echec de la copie: {output}")
                return False
                
        except Exception as e:
            print(f"[ERREUR] Exception lors de la sauvegarde: {e}")
            return False
        finally:
            # Nettoyer le fichier temporaire
            if os.path.exists(temp_path):
                os.remove(temp_path)
    
    def enrich_sensor_data(self, sensor_data):
        """
        Enrichit les données capteurs avec les informations de prédiction
        
        Args:
            sensor_data: Dictionnaire contenant les données du capteur
            
        Returns:
            Dictionnaire enrichi avec prédiction et alerte
        """
        machine_id = sensor_data['machine_id']
        
        # Récupérer l'alerte correspondante depuis le cache
        alert_info = self.alerts_cache.get(machine_id, {})
        
        # Créer l'enregistrement enrichi
        enriched = {
            # Données capteur originales
            'machine_id': sensor_data['machine_id'],
            'timestamp': sensor_data['timestamp'],
            'vibration': sensor_data['vibration'],
            'temperature': sensor_data['temperature'],
            'pression': sensor_data['pression'],
            'consommation_electrique': sensor_data['consommation_electrique'],
            'charge_travail': sensor_data['charge_travail'],
            
            # Informations de prédiction (si disponibles)
            'alert_id': alert_info.get('alert_id', None),
            'timestamp_detection': alert_info.get('timestamp_detection', None),
            'type_panne': alert_info.get('type_panne', 'aucune'),
            'probabilite_panne': alert_info.get('probabilite_panne', 0.0),
            'action_recommandee': alert_info.get('action_recommandee', 'CONTINUER'),
            
            # Features ML (si disponibles)
            'vibration_moyenne_1min': alert_info.get('features_ml', {}).get('vibration_moyenne_1min', None),
            'temperature_moyenne_5min': alert_info.get('features_ml', {}).get('temperature_moyenne_5min', None),
            'pression_ecart_type_10min': alert_info.get('features_ml', {}).get('pression_ecart_type_10min', None),
            
            # Timestamp de sauvegarde
            'timestamp_saved': datetime.now().isoformat()
        }
        
        return enriched
    
    def should_save(self):
        """Détermine si on doit sauvegarder maintenant"""
        time_elapsed = time.time() - self.last_save_time
        return len(self.data_buffer) >= self.batch_size or time_elapsed >= self.save_interval
    
    def process_and_save_buffer(self):
        """Traite et sauvegarde le buffer accumulé"""
        if not self.data_buffer:
            return
        
        print(f"\n{'='*70}")
        print(f"SAUVEGARDE HDFS - {len(self.data_buffer)} enregistrements")
        print(f"{'='*70}")
        
        # Créer un DataFrame
        df = pd.DataFrame(self.data_buffer)
        
        # Partition par date
        partition_date = datetime.now().strftime("%Y-%m-%d")
        
        # Sauvegarder dans HDFS
        if self.save_to_hdfs(df, partition_date):
            print(f"[OK] {len(self.data_buffer)} enregistrements sauvegardes")
            
            # Statistiques
            alerts_count = df[df['type_panne'] != 'aucune'].shape[0]
            alert_rate = (alerts_count / len(df)) * 100 if len(df) > 0 else 0
            print(f"[STATS] Alertes: {alerts_count}/{len(df)} ({alert_rate:.1f}%)")
            
            # Réinitialiser le buffer
            self.data_buffer = []
            self.last_save_time = time.time()
        else:
            print(f"[ERREUR] Echec de la sauvegarde, les donnees restent en buffer")
        
        print(f"{'='*70}\n")
    
    def run_dual_consumer(self):
        """
        Lance deux consumers en parallèle:
        1. Pour les données capteurs (topic principal)
        2. Pour les alertes (pour enrichissement)
        """
        print("="*70)
        print("HDFS DATA SAVER - SYSTEME DE SAUVEGARDE")
        print("="*70)
        print(f"Topics Kafka:")
        print(f"  - Capteurs: {TOPIC_SENSORS}")
        print(f"  - Alertes:  {TOPIC_ALERTS}")
        print(f"Destination HDFS: {HDFS_BASE_PATH}")
        print(f"Batch size: {self.batch_size} enregistrements")
        print(f"Intervalle: {self.save_interval} secondes")
        print("="*70)
        print()
        
        # Consumer pour les alertes (groupe séparé pour recevoir toutes les alertes)
        alert_consumer = KafkaConsumer(
            TOPIC_ALERTS,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='hdfs-alert-cache-group',
            consumer_timeout_ms=1000  # Timeout pour permettre de vérifier l'autre topic
        )
        
        # Consumer pour les données capteurs
        sensor_consumer = KafkaConsumer(
            TOPIC_SENSORS,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='hdfs-data-saver-group',
            consumer_timeout_ms=1000
        )
        
        print("[OK] Connexion etablie a Kafka")
        print("[INFO] En attente de donnees...\n")
        
        total_processed = 0
        total_saved = 0
        
        try:
            while True:
                # 1. Lire les alertes et mettre à jour le cache
                for alert_msg in alert_consumer:
                    alert = alert_msg.value
                    machine_id = alert['machine_id']
                    self.alerts_cache[machine_id] = alert
                    print(f"[CACHE] Alerte recue pour {machine_id}: {alert['type_panne']}")
                
                # 2. Lire les données capteurs et les enrichir
                for sensor_msg in sensor_consumer:
                    sensor_data = sensor_msg.value
                    
                    # Enrichir avec les données de prédiction
                    enriched_data = self.enrich_sensor_data(sensor_data)
                    
                    # Ajouter au buffer
                    self.data_buffer.append(enriched_data)
                    total_processed += 1
                    
                    # Log
                    status = "[DATA]"
                    if enriched_data['type_panne'] != 'aucune':
                        status = f"[ALERT-{enriched_data['action_recommandee']}]"
                    
                    print(f"{status} {sensor_data['machine_id']:15} | Buffer: {len(self.data_buffer)}/{self.batch_size}")
                    
                    # Sauvegarder si nécessaire
                    if self.should_save():
                        self.process_and_save_buffer()
                        total_saved += len(self.data_buffer)
                
        except KeyboardInterrupt:
            print("\n\nArret du systeme...")
            
            # Sauvegarder les données restantes
            if self.data_buffer:
                print(f"[INFO] Sauvegarde des {len(self.data_buffer)} derniers enregistrements...")
                self.process_and_save_buffer()
            
            print(f"\n{'='*70}")
            print(f"STATISTIQUES FINALES")
            print(f"{'='*70}")
            print(f"Total traite:     {total_processed} enregistrements")
            print(f"Total sauvegarde: {total_saved} enregistrements")
            print(f"={'='*70}")
            
            sensor_consumer.close()
            alert_consumer.close()
            print("\n[OK] Termine proprement")


def main():
    """Point d'entrée principal"""
    # Paramètres de configuration
    BATCH_SIZE = 50  # Sauvegarder tous les 50 enregistrements
    SAVE_INTERVAL = 30  # Ou toutes les 30 secondes
    
    saver = HDFSDataSaver(batch_size=BATCH_SIZE, save_interval_seconds=SAVE_INTERVAL)
    saver.run_dual_consumer()


if __name__ == "__main__":
    main()
