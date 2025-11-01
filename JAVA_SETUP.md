# Configuration Java 17 pour PySpark

## Problème

PySpark nécessite Java 11 ou supérieur. Par défaut, votre système utilise Java 8, ce qui cause des erreurs lors de l'exécution des jobs Spark.

## Solution

Vous avez Java 17 installé sur votre machine. Il suffit de configurer les variables d'environnement avant de lancer les jobs Spark.

---

## ✅ Java 17 déjà configuré dans ce terminal !

Java 17 est déjà actif dans votre terminal PowerShell actuel :
- **JAVA_HOME:** `C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot`
- **Version:** OpenJDK 17.0.17

Vous pouvez maintenant exécuter les jobs Spark directement !

---

## Méthode 1 : PowerShell (Recommandé)

Dans votre terminal PowerShell actuel, les variables sont déjà configurées. Pour les futurs terminaux :

```powershell
# Configurer Java 17 (à exécuter une fois par session)
. .\set_java17.ps1
```

---

## Méthode 2 : Batch File

Double-cliquez sur `set_java17.bat` pour ouvrir un nouveau terminal avec Java 17 configuré.

Ou depuis PowerShell :
```powershell
.\set_java17.bat
```

---

## Méthode 3 : Configuration manuelle (PowerShell)

```powershell
# Définir JAVA_HOME
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"

# Mettre à jour PATH
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

# Vérifier
java -version
```

---

## Méthode 4 : Configuration manuelle (CMD)

```cmd
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot
set PATH=%JAVA_HOME%\bin;%PATH%
java -version
```

---

## Vérification

Pour vérifier que Java 17 est actif :

```powershell
# Vérifier JAVA_HOME
echo $env:JAVA_HOME

# Vérifier la version Java
java -version
```

**Résultat attendu :**
```
openjdk version "17.0.17" 2025-10-21
OpenJDK Runtime Environment Temurin-17.0.17+10 (build 17.0.17+10)
OpenJDK 64-Bit Server VM Temurin-17.0.17+10 (build 17.0.17+10, mixed mode, sharing)
```

---

## Workflow pour lancer les jobs Spark

### Entraînement du modèle

```powershell
# 1. Configurer Java 17 (si pas déjà fait)
. .\set_java17.ps1

# 2. Entraîner le modèle
python src/spark_jobs/maintenance_predictor.py --mode training --training-data data/training_data.parquet
```

### Prédictions en streaming

```powershell
# Terminal 1 : Simulateur
python src/simulators/simulateur_capteurs.py --interval 2

# Terminal 2 : Prédictions Spark
. .\set_java17.ps1  # Configurer Java 17
python src/spark_jobs/maintenance_predictor.py --mode streaming
```

---

## Configuration permanente (Optionnel)

Pour définir Java 17 de manière permanente comme version par défaut :

### Via Variables d'Environnement Système

1. Ouvrir **Paramètres système avancés**
2. Cliquer sur **Variables d'environnement**
3. Dans **Variables système**, créer/modifier :
   - Variable : `JAVA_HOME`
   - Valeur : `C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot`
4. Dans **Path**, ajouter en **premier** :
   - `%JAVA_HOME%\bin`
5. Redémarrer le terminal

---

## Résolution des problèmes

### "java : Le terme 'java' n'est pas reconnu"
→ Java n'est pas dans le PATH. Exécutez `set_java17.ps1`

### "Unsupported class file major version 61"
→ Vous utilisez toujours Java 8. Vérifiez avec `java -version`

### "JAVA_HOME is not set"
→ Exécutez `$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"`

---

## Versions Java disponibles sur votre machine

```
C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot  ← UTILISÉE
C:\Program Files\Eclipse Adoptium\jdk-25.0.1.8-hotspot
C:\Program Files\Java\jdk1.8.0_202
```

---

**Note :** Les variables d'environnement configurées avec PowerShell (`$env:`) sont temporaires et valables uniquement pour la session en cours. Pour une configuration permanente, utilisez les Variables d'Environnement Système Windows.
