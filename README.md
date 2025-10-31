# Projet de Maintenance et Optimisation Industrielle Intelligente

Ce projet vise à construire une plateforme Big Data complète pour la supervision d'un parc de machines industrielles.

**Objectifs :**
1.  **Maintenance Prédictive :** Prédire les pannes de machines avant qu'elles ne surviennent.
2.  **Optimisation Énergétique :** Détecter la surconsommation et les fuites d'énergie.
3.  **Orchestration Intelligente :** Gérer dynamiquement le parc de machines en fonction de la charge de travail.
4.  **Supervision en Temps Réel :** Fournir un tableau de bord centralisé pour les opérateurs.

## Architecture Finale


*Le diagramme détaillé de notre architecture microservices et élastique.*

## Stack Technologique

| Domaine | Technologie | Version Suggérée | Rôle |
| :--- | :--- | :--- | :--- |
| Langage Principal | Python | 3.9+ | Pour les simulateurs, l'actionneur et les jobs Spark (PySpark). |
| Ingestion & Messagerie | Apache Kafka | 3.x | Le bus de communication central entre tous les microservices. |
| Traitement Distribué | Apache Spark | 3.3+ | Le moteur de calcul pour tous les jobs de traitement (batch & streaming). |
| Gestion des Ressources | Hadoop YARN | 3.x | Le "cerveau" qui alloue les ressources du cluster aux jobs Spark (gère l'élasticité). |
| Stockage (Data Lake) | HDFS | 3.x | Stockage à long terme des données brutes au format **Parquet**. |
| Stockage (Serving DB) | InfluxDB | 2.x | Base de données optimisée pour les séries temporelles, pour les données "chaudes" du dashboard. |
| Visualisation & Alerting | Grafana | 9.x | Le cockpit de supervision pour l'utilisateur final. |

## Guide d'Installation (Recommandation Forte)

Pour simplifier la mise en place de cet environnement complexe pour toute l'équipe, il est **fortement recommandé d'utiliser Docker et Docker Compose**.

Un seul fichier `docker-compose.yml` peut démarrer tout l'écosystème (Kafka, Zookeeper, Spark, Hadoop/HDFS, InfluxDB, Grafana) sur n'importe quelle machine. Cela garantit que tout le monde travaille avec la même configuration.

## Fichiers de Spécifications Détaillées

*   **[SPECIFICATIONS.md](SPECIFICATIONS.md) :** **À LIRE EN PREMIER.** Contient le contrat de données : la structure exacte de tous les topics Kafka, des tables InfluxDB et du stockage HDFS.
*   **[ML_MODEL_DESIGN.md](ML_MODEL_DESIGN.md) :** Guide pour la conception et l'entraînement du modèle de maintenance prédictive.
*   **[PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) :** Proposition de l'arborescence des fichiers du projet pour organiser le code.