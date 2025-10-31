# Conception du Modèle de Maintenance Prédictive

Ce document guide le développement du microservice `maintenance_predictor`.

## 1. Problème Métier

Nous voulons prédire la probabilité qu'une machine tombe en panne dans les prochaines 24 heures. C'est un problème de **classification binaire** :
- **Classe 0 :** Pas de panne imminente.
- **Classe 1 :** Panne probable dans les 24h.

## 2. Feature Engineering (Job Batch Spark)

La performance du modèle dépendra de la qualité des features. Le job d'entraînement doit créer ces features à partir des données brutes de HDFS.

**Idées de Features à créer pour chaque `timestamp` :**
- **Fenêtres Glissantes (Rolling Windows) :** Calculer des agrégats sur différentes périodes de temps avant le `timestamp` actuel.
  - `vibration_moyenne_1min`, `vibration_moyenne_5min`, `vibration_moyenne_1h`
  - `temperature_moyenne_5min`, `temperature_max_1h`
  - `pression_ecart_type_10min`
- **Lag Features :** Comparer la valeur actuelle à une valeur passée.
  - `diff_temp_vs_10min_ago`
- **Indice de Santé Composite (Exemple avancé) :** Créer un score simple, par exemple : `(vibration_normalisée * 0.5) + (temperature_normalisée * 0.3) + (pression_normalisée * 0.2)`.

Le **job de Streaming** devra être capable de calculer **exactement les mêmes features** en temps réel.

## 3. Choix et Entraînement du Modèle

- **Algorithme Suggéré :**
  1.  **RandomForestClassifier (Forêt Aléatoire) :** Excellent point de départ. Robuste, performant et fournit une "importance des features" pour comprendre ce qui influence les pannes.
  2.  **GradientBoostedTreeClassifier (GBT) :** Souvent plus performant mais plus long à entraîner et plus sensible aux hyperparamètres.
- **Librairie :** **Spark MLlib**. Elle est conçue pour fonctionner sur des données distribuées et s'intègre nativement avec les DataFrames Spark.
- **Processus d'Entraînement :**
  1.  Lire les données partitionnées de HDFS.
  2.  Créer le DataFrame de features.
  3.  Créer la colonne "label" (`panne_dans_24h`). (Dans la simulation, il faudra générer des pannes pour avoir des labels positifs).
  4.  Diviser les données : 80% pour l'entraînement, 20% pour la validation.
  5.  Utiliser un `VectorAssembler` de Spark MLlib pour regrouper toutes les features dans une seule colonne.
  6.  Entraîner le modèle.
  7.  Évaluer la performance (ex: Aire sous la courbe ROC, Précision, Rappel).
  8.  **Sauvegarder le modèle final sur HDFS.**

## 4. Prédiction en Streaming

Le job de streaming chargera le modèle sauvegardé une seule fois au démarrage pour des prédictions rapides.