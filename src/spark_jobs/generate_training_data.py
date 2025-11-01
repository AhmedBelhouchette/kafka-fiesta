"""
Générateur de données d'entraînement pour le modèle de maintenance prédictive
Crée un dataset avec des labels (aucune, pause_requise, maintenance_requise)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import argparse
import os


def generate_normal_data(n_samples: int = 1000) -> pd.DataFrame:
    """Génère des données de fonctionnement normal (aucune panne)"""
    data = []
    machines = ["POMPE-01", "POMPE-02", "POMPE-03", "MOTEUR-01", "MOTEUR-02", "COMPRESSEUR-01"]
    
    for _ in range(n_samples):
        machine = random.choice(machines)
        timestamp = datetime.now() - timedelta(seconds=random.randint(0, 86400*30))
        
        data.append({
            "machine_id": machine,
            "timestamp": timestamp.isoformat() + "Z",
            "etat": "en_marche",
            "charge_travail": round(random.uniform(60, 85), 2),
            "consommation_electrique": round(random.uniform(12, 18), 2),
            "vibration": round(random.uniform(0.5, 1.5), 3),
            "temperature": round(random.uniform(40, 60), 2),
            "pression": round(random.uniform(140, 160), 2),
            # Features calculées (simulées)
            "vibration_moyenne_1min": round(random.uniform(0.5, 1.5), 3),
            "temperature_moyenne_1min": round(random.uniform(40, 60), 2),
            "pression_moyenne_1min": round(random.uniform(140, 160), 2),
            "consommation_moyenne_1min": round(random.uniform(12, 18), 2),
            "charge_moyenne_1min": round(random.uniform(60, 85), 2),
            "vibration_ecart_type_1min": round(random.uniform(0.1, 0.3), 3),
            "pression_ecart_type_1min": round(random.uniform(1, 3), 2),
            "temperature_max_1min": round(random.uniform(40, 62), 2),
            # Label
            "type_panne": "aucune"
        })
    
    return pd.DataFrame(data)


def generate_degraded_data(n_samples: int = 500) -> pd.DataFrame:
    """Génère des données de fonctionnement dégradé (pause requise)"""
    data = []
    machines = ["POMPE-01", "POMPE-02", "POMPE-03", "MOTEUR-01", "MOTEUR-02", "COMPRESSEUR-01"]
    
    for _ in range(n_samples):
        machine = random.choice(machines)
        timestamp = datetime.now() - timedelta(seconds=random.randint(0, 86400*30))
        
        data.append({
            "machine_id": machine,
            "timestamp": timestamp.isoformat() + "Z",
            "etat": "en_marche",
            "charge_travail": round(random.uniform(85, 95), 2),
            "consommation_electrique": round(random.uniform(18, 24), 2),
            "vibration": round(random.uniform(1.8, 2.5), 3),
            "temperature": round(random.uniform(65, 78), 2),
            "pression": round(random.uniform(135, 148), 2),
            # Features calculées (simulées)
            "vibration_moyenne_1min": round(random.uniform(1.8, 2.5), 3),
            "temperature_moyenne_1min": round(random.uniform(65, 78), 2),
            "pression_moyenne_1min": round(random.uniform(135, 148), 2),
            "consommation_moyenne_1min": round(random.uniform(18, 24), 2),
            "charge_moyenne_1min": round(random.uniform(85, 95), 2),
            "vibration_ecart_type_1min": round(random.uniform(0.3, 0.6), 3),
            "pression_ecart_type_1min": round(random.uniform(3, 6), 2),
            "temperature_max_1min": round(random.uniform(70, 82), 2),
            # Label
            "type_panne": "pause_requise"
        })
    
    return pd.DataFrame(data)


def generate_critical_data(n_samples: int = 300) -> pd.DataFrame:
    """Génère des données critiques (maintenance requise)"""
    data = []
    machines = ["POMPE-01", "POMPE-02", "POMPE-03", "MOTEUR-01", "MOTEUR-02", "COMPRESSEUR-01"]
    
    for _ in range(n_samples):
        machine = random.choice(machines)
        timestamp = datetime.now() - timedelta(seconds=random.randint(0, 86400*30))
        
        data.append({
            "machine_id": machine,
            "timestamp": timestamp.isoformat() + "Z",
            "etat": "en_marche",
            "charge_travail": round(random.uniform(95, 100), 2),
            "consommation_electrique": round(random.uniform(24, 35), 2),
            "vibration": round(random.uniform(3.0, 5.0), 3),
            "temperature": round(random.uniform(80, 100), 2),
            "pression": round(random.uniform(120, 135), 2),
            # Features calculées (simulées)
            "vibration_moyenne_1min": round(random.uniform(3.0, 5.0), 3),
            "temperature_moyenne_1min": round(random.uniform(80, 100), 2),
            "pression_moyenne_1min": round(random.uniform(120, 135), 2),
            "consommation_moyenne_1min": round(random.uniform(24, 35), 2),
            "charge_moyenne_1min": round(random.uniform(95, 100), 2),
            "vibration_ecart_type_1min": round(random.uniform(0.6, 1.2), 3),
            "pression_ecart_type_1min": round(random.uniform(6, 15), 2),
            "temperature_max_1min": round(random.uniform(90, 105), 2),
            # Label
            "type_panne": "maintenance_requise"
        })
    
    return pd.DataFrame(data)


def generate_training_dataset(
    n_normal: int = 1000,
    n_degraded: int = 500,
    n_critical: int = 300,
    output_format: str = "csv"
) -> pd.DataFrame:
    """
    Génère un dataset d'entraînement complet
    
    Args:
        n_normal: Nombre d'exemples normaux
        n_degraded: Nombre d'exemples dégradés
        n_critical: Nombre d'exemples critiques
        output_format: "csv" ou "parquet"
    
    Returns:
        DataFrame complet
    """
    print("Generation des donnees d'entrainement...")
    
    # Générer les données
    df_normal = generate_normal_data(n_normal)
    print(f"Donnees normales: {len(df_normal)} lignes")
    
    df_degraded = generate_degraded_data(n_degraded)
    print(f"Donnees degradees: {len(df_degraded)} lignes")
    
    df_critical = generate_critical_data(n_critical)
    print(f"Donnees critiques: {len(df_critical)} lignes")
    
    # Combiner et mélanger
    df_combined = pd.concat([df_normal, df_degraded, df_critical], ignore_index=True)
    df_combined = df_combined.sample(frac=1, random_state=42).reset_index(drop=True)
    
    print(f"Dataset total: {len(df_combined)} lignes")
    print("\nDistribution des classes:")
    class_counts = df_combined['type_panne'].value_counts()
    print(class_counts)
    print("\nPourcentages:")
    print((class_counts / len(df_combined) * 100).round(2))
    print(f"\nTaux d'alertes (pause + maintenance): {((len(df_degraded) + len(df_critical)) / len(df_combined) * 100):.2f}%")
    
    return df_combined


def main():
    parser = argparse.ArgumentParser(
        description="Génère des données d'entraînement pour le modèle de maintenance"
    )
    parser.add_argument(
        "--n-normal",
        type=int,
        default=10000,
        help="Nombre d'exemples normaux (95-97% du dataset)"
    )
    parser.add_argument(
        "--n-degraded",
        type=int,
        default=200,
        help="Nombre d'exemples dégradés (2-3% du dataset)"
    )
    parser.add_argument(
        "--n-critical",
        type=int,
        default=100,
        help="Nombre d'exemples critiques (1% du dataset)"
    )
    parser.add_argument(
        "--output",
        default="data/training_data",
        help="Chemin de sortie (sans extension)"
    )
    parser.add_argument(
        "--format",
        choices=["csv", "parquet"],
        default="parquet",
        help="Format de sortie"
    )
    
    args = parser.parse_args()
    
    print("="*70)
    print("GENERATEUR DE DONNEES D'ENTRAINEMENT")
    print("="*70)
    
    # Générer le dataset
    df = generate_training_dataset(
        n_normal=args.n_normal,
        n_degraded=args.n_degraded,
        n_critical=args.n_critical
    )
    
    # Créer le dossier de sortie si nécessaire
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Sauvegarder
    if args.format == "csv":
        output_path = f"{args.output}.csv"
        df.to_csv(output_path, index=False)
    else:
        output_path = f"{args.output}.parquet"
        df.to_parquet(output_path, index=False)
    
    print(f"\nDonnees sauvegardees dans: {output_path}")
    print("="*70)
    
    # Afficher quelques statistiques
    print("\nStatistiques descriptives:")
    print(df[['vibration', 'temperature', 'pression', 'charge_travail']].describe())


if __name__ == "__main__":
    main()
