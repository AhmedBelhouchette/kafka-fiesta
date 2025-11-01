# Script pour configurer Java 17 et Hadoop pour PySpark
# Executer: . .\set_java17.ps1

Write-Host "Configuration de Java 17 et Hadoop pour PySpark" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

# Definir JAVA_HOME pour Java 17
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"

# Ajouter Java 17 au debut du PATH
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

# Definir HADOOP_HOME (workaround pour Windows)
$env:HADOOP_HOME = "C:\hadoop"

# Creer le dossier si necessaire
if (-not (Test-Path "$env:HADOOP_HOME\bin")) {
    New-Item -ItemType Directory -Path "$env:HADOOP_HOME\bin" -Force | Out-Null
    Write-Host "Dossier Hadoop cree: $env:HADOOP_HOME\bin" -ForegroundColor Yellow
}

# Verifier la configuration
Write-Host ""
Write-Host "JAVA_HOME defini:" -ForegroundColor Green
Write-Host $env:JAVA_HOME -ForegroundColor Yellow

Write-Host ""
Write-Host "HADOOP_HOME defini:" -ForegroundColor Green
Write-Host $env:HADOOP_HOME -ForegroundColor Yellow

Write-Host ""
Write-Host "Version Java active:" -ForegroundColor Green
java -version

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "Configuration terminee !" -ForegroundColor Green
Write-Host "Vous pouvez maintenant executer les jobs Spark" -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Cyan
