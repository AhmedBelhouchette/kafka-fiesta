# SPECIFICATIONS

Fichier de spécifications fonctionnelles et non-fonctionnelles.
- Compléter les cas d'utilisation
- Décrire les topics Kafka attendus
- Définir SLAs et objectifs modèle ML

# Spécifications Techniques des Structures de Données

Ce document est la **source de vérité unique** pour toutes les structures de données du projet. Tout le code doit impérativement respecter ces schémas pour assurer l'interopérabilité des microservices.

## 1. Topics Apache Kafka

### Topic: `donnees-capteurs`
- **Rôle :** Flux principal des mesures brutes de toutes les machines.
- **Format :** JSON
- **Schéma :**
```json
{
  "machine_id": "string",
  "timestamp": "string",
  "etat": "string",
  "charge_travail": "float",
  "consommation_electrique": "float",
  "vibration": "float",
  "temperature": "float",
  "pression": "float"
}
```
- **Exemple :** `{"machine_id": "POMPE-01", "timestamp": "2025-10-31T10:05:12.345Z", "etat": "en_marche", "charge_travail": 85.5, "consommation_electrique": 15.2, "vibration": 1.25, "temperature": 55.3, "pression": 150.7}`

---
### Topic: `plan-de-production`
- **Rôle :** Contient les ordres de production à réaliser.
- **Format :** JSON
- **Schéma :**
```json
{
  "tache_id": "string",
  "type_produit": "string",
  "quantite": "integer"
}
```
- **Exemple :** `{"tache_id": "TASK-501AE", "type_produit": "PRODUIT_A", "quantite": 5000}`

---
### Topic: `alertes-pannes`
- **Rôle :** Publie les alertes générées par le service de maintenance prédictive.
- **Format :** JSON
- **Schéma :**
```json
{
  "machine_id": "string",
  "probabilite_panne": "float",
  "timestamp_detection": "string"
}
```
- **Exemple :** `{"machine_id": "POMPE-04", "probabilite_panne": 0.92, "timestamp_detection": "2025-10-31T10:06:00.000Z"}`

---
### topic de sortie 
### Topic: `alertes-energie`
- **Rôle :** Publie les alertes de fuite ou de surconsommation.
- **Format :** JSON
- **Schéma :**
```json
{
  "machine_id": "string",
  "type_alerte": "string",
  "valeur_actuelle": "float",
  "valeur_normale": "float",
  "timestamp_detection": "string"
}
```
- **Exemple :** `{"machine_id": "MOTEUR-02", "type_alerte": "surconsommation", "valeur_actuelle": 22.5, "valeur_normale": 18.0, "timestamp_detection": "2025-10-31T10:07:00.000Z"}`

---
### Topic: `commandes-machines`
- **Rôle :** Contient les ordres de démarrage/arrêt envoyés par l'orchestrateur.
- **Format :** JSON
- **Schéma :**
```json
{
  "machine_id": "string",
  "action": "string"
}
```
- **Exemple :** `{"machine_id": "POMPE-07", "action": "DEMARRER"}`

## 2. Stockage HDFS (Data Lake)

- **Rôle :** Archivage à long terme de toutes les données brutes pour l'entraînement des modèles.
- **Format :** **Parquet** (Les jobs Spark doivent convertir le JSON de Kafka en Parquet).
- **Structure de dossiers (Partitionnement) :** Les données doivent être partitionnées par date pour des lectures efficaces.
  ```
  /data/raw/donnees-capteurs/
  └── year=2025/
      └── month=10/
          └── day=31/
              └── part-00000-....c000.snappy.parquet
  ```
- **Schéma des fichiers Parquet :** Identique au schéma JSON du topic `donnees-capteurs`, mais avec des types de données Spark SQL.
  ```
  root
   |-- machine_id: string (nullable = true)
   |-- timestamp: timestamp (nullable = true)
   |-- etat: string (nullable = true)
   |-- charge_travail: double (nullable = true)
   |-- consommation_electrique: double (nullable = true)
   |-- vibration: double (nullable = true)
   |-- temperature: double (nullable = true)
   |-- pression: double (nullable = true)
  ```

## 3. Base InfluxDB (Serving Layer)

- **Rôle :** Stocker les données "chaudes" agrégées pour l'affichage rapide dans Grafana.
- **Terminologie InfluxDB :**
    - Un **"Measurement"** est comme une table SQL.
    - Un **"Tag"** est une colonne indexée (utilisée pour les clauses `WHERE` et `GROUP BY`).
    - Un **"Field"** est une colonne de données (les valeurs que l'on veut afficher).

### Measurement: `etat_machines`
- **Description :** L'état en temps réel de chaque machine.
- **Tags :** `machine_id`
- **Fields :** `etat` (string), `charge_travail` (float), `temperature` (float), `vibration` (float), `consommation_electrique` (float)

### Measurement: `predictions`
- **Description :** Les prédictions de pannes en continu.
- **Tags :** `machine_id`
- **Fields :** `probabilite_panne` (float)

### Measurement: `anomalies_energie`
- **Description :** Un enregistrement de chaque anomalie énergétique détectée.
- **Tags :** `machine_id`, `type_alerte` ("surconsommation" ou "fuite")
- **Fields :** `valeur_actuelle` (float), `valeur_normale` (float)