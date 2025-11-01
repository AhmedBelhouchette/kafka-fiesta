"""
Script de configuration de l'infrastructure pour le projet de maintenance prédictive
Ce script automatise la création des topics Kafka et des dossiers HDFS nécessaires
"""

import subprocess
import sys
from typing import List, Tuple


class InfrastructureSetup:
    """Classe pour gérer la configuration de l'infrastructure"""
    
    def __init__(self):
        self.kafka_container = "kafka"
        self.hdfs_container = "hadoop-master"
        
    def run_docker_command(self, container: str, command: List[str]) -> Tuple[bool, str]:
        """
        Exécute une commande dans un conteneur Docker
        
        Args:
            container: Nom du conteneur Docker
            command: Liste des arguments de la commande
            
        Returns:
            Tuple (succès, message)
        """
        try:
            cmd = ["docker", "exec", container] + command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            return True, result.stdout
        except subprocess.CalledProcessError as e:
            return False, f"Erreur: {e.stderr}"
    
    def list_kafka_topics(self) -> List[str]:
        """Liste tous les topics Kafka existants"""
        success, output = self.run_docker_command(
            self.kafka_container,
            ["kafka-topics", "--list", "--bootstrap-server", "localhost:9092"]
        )
        if success:
            topics = [t.strip() for t in output.split('\n') if t.strip() and not t.startswith('__')]
            return topics
        return []
    
    def delete_topic(self, topic_name: str) -> bool:
        """Supprime un topic Kafka"""
        print(f"Suppression du topic '{topic_name}'...")
        success, message = self.run_docker_command(
            self.kafka_container,
            ["kafka-topics", "--delete", "--topic", topic_name, 
             "--bootstrap-server", "localhost:9092"]
        )
        if success:
            print(f"Topic '{topic_name}' supprime")
        else:
            print(f"ATTENTION: {message}")
        return success
    
    def create_kafka_topic(self, topic_name: str, partitions: int = 3, 
                          replication_factor: int = 1) -> bool:
        """Crée un topic Kafka"""
        print(f"📝 Création du topic '{topic_name}' avec {partitions} partitions...")
        success, message = self.run_docker_command(
            self.kafka_container,
            [
                "kafka-topics", "--create",
                "--topic", topic_name,
                "--bootstrap-server", "localhost:9092",
                "--partitions", str(partitions),
                "--replication-factor", str(replication_factor)
            ]
        )
        if success:
            print(f"Topic '{topic_name}' cree avec succes")
        else:
            print(f"ERREUR: Echec: {message}")
        return success
    
    def describe_kafka_topic(self, topic_name: str):
        """Affiche les détails d'un topic Kafka"""
        success, output = self.run_docker_command(
            self.kafka_container,
            ["kafka-topics", "--describe", "--topic", topic_name,
             "--bootstrap-server", "localhost:9092"]
        )
        if success:
            print(f"\nConfiguration du topic '{topic_name}':")
            print(output)
    
    def create_hdfs_directory(self, path: str) -> bool:
        """Crée un répertoire dans HDFS"""
        print(f"Creation du repertoire HDFS '{path}'...")
        success, message = self.run_docker_command(
            self.hdfs_container,
            ["hdfs", "dfs", "-mkdir", "-p", path]
        )
        if success or "File exists" in message:
            print(f"Repertoire '{path}' pret")
            return True
        else:
            print(f"ERREUR: Echec: {message}")
            return False
    
    def list_hdfs_directory(self, path: str = "/data"):
        """Liste le contenu d'un répertoire HDFS"""
        success, output = self.run_docker_command(
            self.hdfs_container,
            ["hdfs", "dfs", "-ls", "-R", path]
        )
        if success:
            print(f"\n📂 Contenu de HDFS '{path}':")
            print(output)
    
    def setup_kafka_topics(self, recreate: bool = False):
        """Configure tous les topics Kafka nécessaires"""
        print("\n" + "="*60)
        print("🚀 CONFIGURATION DES TOPICS KAFKA")
        print("="*60 + "\n")
        
        topics_config = [
            ("donnees-capteurs", 3),
            ("alertes-maintenance", 2),
        ]
        
        # Lister les topics existants
        existing_topics = self.list_kafka_topics()
        print(f"📋 Topics existants: {', '.join(existing_topics) if existing_topics else 'Aucun'}\n")
        
        for topic_name, partitions in topics_config:
            # Supprimer si demandé et si existe
            if recreate and topic_name in existing_topics:
                self.delete_kafka_topic(topic_name)
            
            # Créer le topic
            self.create_kafka_topic(topic_name, partitions)
            
            # Afficher la configuration
            self.describe_kafka_topic(topic_name)
    
    def setup_hdfs_structure(self):
        """Configure la structure de répertoires dans HDFS"""
        print("\n" + "="*60)
        print("CONFIGURATION DE LA STRUCTURE HDFS")
        print("="*60 + "\n")
        
        directories = [
            "/data/raw/donnees-capteurs",
            "/data/processed/features",
            "/data/models/maintenance_predictor",
            "/data/predictions/maintenance"
        ]
        
        for directory in directories:
            self.create_hdfs_directory(directory)
        
        # Afficher la structure créée
        self.list_hdfs_directory("/data")
    
    def verify_infrastructure(self) -> bool:
        """Vérifie que l'infrastructure est prête"""
        print("\n" + "="*60)
        print("🔍 VÉRIFICATION DE L'INFRASTRUCTURE")
        print("="*60 + "\n")
        
        # Vérifier les conteneurs Docker
        containers = ["kafka", "zookeeper", "hadoop-master", "influxdb"]
        all_running = True
        
        for container in containers:
            try:
                result = subprocess.run(
                    ["docker", "ps", "--filter", f"name={container}", "--format", "{{.Names}}"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                if container in result.stdout:
                    print(f"Conteneur '{container}' est actif")
                else:
                    print(f"ERREUR: Conteneur '{container}' n'est pas actif")
                    all_running = False
            except subprocess.CalledProcessError:
                print(f"ERREUR: Impossible de verifier le conteneur '{container}'")
                all_running = False
        
        return all_running
    
    def run_full_setup(self, recreate_topics: bool = False):
        """Exécute la configuration complète de l'infrastructure"""
        print("\n" + "="*60)
        print("CONFIGURATION COMPLETE DE L'INFRASTRUCTURE")
        print("="*60 + "\n")
        
        # Vérifier l'infrastructure
        if not self.verify_infrastructure():
            print("\nATTENTION: Certains conteneurs ne sont pas actifs. Veuillez demarrer Docker Compose.")
            return False
        
        # Configurer Kafka
        self.setup_kafka_topics(recreate=recreate_topics)
        
        # Configurer HDFS
        self.setup_hdfs_structure()
        
        print("\n" + "="*60)
        print("CONFIGURATION TERMINEE AVEC SUCCES")
        print("="*60 + "\n")
        
        return True


def main():
    """Point d'entrée principal du script"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Configuration de l'infrastructure pour le projet de maintenance prédictive"
    )
    parser.add_argument(
        "--recreate-topics",
        action="store_true",
        help="Supprimer et recréer les topics Kafka existants"
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Vérifier uniquement l'état de l'infrastructure"
    )
    
    args = parser.parse_args()
    
    setup = InfrastructureSetup()
    
    if args.verify_only:
        setup.verify_infrastructure()
    else:
        setup.run_full_setup(recreate_topics=args.recreate_topics)


if __name__ == "__main__":
    main()
