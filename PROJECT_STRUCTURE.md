# Structure Suggérée des Dossiers du Projet

Pour maintenir le projet organisé, voici une proposition d'arborescence de fichiers.

```
/projet-maintenance-industrielle/
|
├── docker-compose.yml         # Fichier pour lancer toute la stack infra
|
├── data/                      # Données locales (si nécessaire)
|
├── notebooks/                 # Notebooks Jupyter/Zeppelin pour l'exploration
|   └── 01-exploration-donnees.ipynb
|
├── src/                       # Tout le code source
|   |
|   ├── simulators/            # Les scripts de simulation de données
|   |   ├── simulateur_capteurs.py
|   |   └── simulateur_production.py
|   |
|   ├── spark_jobs/            # Les jobs Spark (le coeur du projet)
|   |   ├── common/            # Code partagé (ex: connexion à Kafka)
|   |   |   └── spark_session.py
|   |   |
|   |   ├── maintenance_predictor.py
|   |   ├── energy_monitor.py
|   |   └── production_orchestrator.py
|   |
|   └── services/              # Autres petits services
|       └── actionneur.py
|
└── README.md                  # Fichiers de documentation
└── SPECIFICATIONS.md
└── ML_MODEL_DESIGN.md
└── PROJECT_STRUCTURE.md
```