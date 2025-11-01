"""
Script de test pour le HDFS Data Saver
Lance tous les composants et affiche les statistiques
"""

import subprocess
import time
import sys
import os

def check_docker_containers():
    """Vérifie que les containers nécessaires sont actifs"""
    print("="*70)
    print("VERIFICATION DE L'INFRASTRUCTURE")
    print("="*70)
    
    required_containers = {
        'kafka': 'Kafka broker',
        'hadoop-master': 'Hadoop/HDFS',
        'zookeeper': 'Zookeeper'
    }
    
    result = subprocess.run(['docker', 'ps', '--format', '{{.Names}}'], 
                          capture_output=True, text=True)
    running_containers = result.stdout.strip().split('\n')
    
    all_running = True
    for container, desc in required_containers.items():
        if container in running_containers:
            print(f"[OK] {desc:20} ({container})")
        else:
            print(f"[ERREUR] {desc:20} ({container}) - NON DEMARRE")
            all_running = False
    
    print()
    return all_running

def check_kafka_topics():
    """Vérifie que les topics Kafka existent"""
    print("="*70)
    print("VERIFICATION DES TOPICS KAFKA")
    print("="*70)
    
    result = subprocess.run([
        'docker', 'exec', 'kafka',
        'kafka-topics', '--list', '--bootstrap-server', 'localhost:9092'
    ], capture_output=True, text=True)
    
    topics = result.stdout.strip().split('\n')
    required_topics = ['donnees-capteurs', 'alertes-maintenance']
    
    all_present = True
    for topic in required_topics:
        if topic in topics:
            print(f"[OK] Topic '{topic}' existe")
        else:
            print(f"[ERREUR] Topic '{topic}' manquant")
            all_present = False
    
    print()
    return all_present

def check_hdfs_directory():
    """Vérifie que le répertoire HDFS existe"""
    print("="*70)
    print("VERIFICATION DE HDFS")
    print("="*70)
    
    result = subprocess.run([
        'docker', 'exec', 'hadoop-master',
        'hdfs', 'dfs', '-test', '-d', '/data/predictions/maintenance'
    ], capture_output=True)
    
    if result.returncode == 0:
        print("[OK] Repertoire /data/predictions/maintenance existe")
        
        # Vérifier les permissions
        result = subprocess.run([
            'docker', 'exec', 'hadoop-master',
            'hdfs', 'dfs', '-ls', '/data/predictions'
        ], capture_output=True, text=True)
        print(result.stdout)
        return True
    else:
        print("[ERREUR] Repertoire /data/predictions/maintenance manquant")
        print("Executez: python src/spark_jobs/setup_infrastructure.py")
        return False

def check_models():
    """Vérifie que les modèles ML existent"""
    print("="*70)
    print("VERIFICATION DES MODELES ML")
    print("="*70)
    
    model_files = ['models/rf_model.pkl', 'models/scaler.pkl']
    all_present = True
    
    for model_file in model_files:
        if os.path.exists(model_file):
            size = os.path.getsize(model_file) / 1024  # KB
            print(f"[OK] {model_file} ({size:.1f} KB)")
        else:
            print(f"[ERREUR] {model_file} manquant")
            all_present = False
    
    if not all_present:
        print("\nGenerez les modeles avec:")
        print("  python src/spark_jobs/generate_training_data.py")
        print("  python src/spark_jobs/ml_predictor_integrated.py")
    
    print()
    return all_present

def show_hdfs_content():
    """Affiche le contenu de HDFS"""
    print("\n" + "="*70)
    print("CONTENU DE HDFS - /data/predictions/maintenance")
    print("="*70)
    
    result = subprocess.run([
        'docker', 'exec', 'hadoop-master',
        'hdfs', 'dfs', '-ls', '-R', '/data/predictions/maintenance'
    ], capture_output=True, text=True)
    
    output = result.stdout.strip()
    if output:
        print(output)
        
        # Calculer la taille totale
        result = subprocess.run([
            'docker', 'exec', 'hadoop-master',
            'hdfs', 'dfs', '-du', '-s', '-h', '/data/predictions/maintenance'
        ], capture_output=True, text=True)
        print(f"\nTaille totale: {result.stdout.strip()}")
    else:
        print("[INFO] Aucun fichier encore sauvegarde")
    
    print()

def print_usage():
    """Affiche les instructions d'utilisation"""
    print("\n" + "="*70)
    print("INSTRUCTIONS DE LANCEMENT")
    print("="*70)
    print("""
Pour tester le système complet, ouvrez 4 terminaux:

Terminal 1 - Simulateur de capteurs:
  python src/simulators/simulateur_capteurs.py

Terminal 2 - Prédicteur ML:
  python src/spark_jobs/ml_predictor_integrated.py

Terminal 3 - HDFS Data Saver:
  python src/spark_jobs/hdfs_data_saver.py

Terminal 4 (Optionnel) - Resource Manager:
  python src/services/resource_manager_consumer.py

Laissez tourner quelques minutes, puis vérifiez HDFS avec:
  python src/spark_jobs/test_hdfs_saver.py --show-content
""")

def main():
    """Point d'entrée principal"""
    args = sys.argv[1:]
    
    if '--show-content' in args:
        show_hdfs_content()
        return
    
    print("\n" + "="*70)
    print("TEST HDFS DATA SAVER - VERIFICATION SYSTEME")
    print("="*70)
    print()
    
    # Vérifications
    checks = [
        ("Docker containers", check_docker_containers),
        ("Topics Kafka", check_kafka_topics),
        ("Repertoire HDFS", check_hdfs_directory),
        ("Modeles ML", check_models)
    ]
    
    all_ok = True
    for name, check_func in checks:
        if not check_func():
            all_ok = False
            time.sleep(1)
    
    # Résumé
    print("="*70)
    print("RESUME DE LA VERIFICATION")
    print("="*70)
    
    if all_ok:
        print("[SUCCESS] Tous les composants sont prets!")
        print_usage()
        show_hdfs_content()
    else:
        print("[ERREUR] Certains composants manquent")
        print("\nCorrigez les problemes ci-dessus avant de continuer.")
        sys.exit(1)

if __name__ == "__main__":
    main()
