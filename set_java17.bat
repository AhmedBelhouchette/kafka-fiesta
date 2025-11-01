@echo off
REM Script batch pour configurer Java 17 pour PySpark

echo ======================================================================
echo Configuration de Java 17 pour PySpark
echo ======================================================================

REM Definir JAVA_HOME pour Java 17
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot

REM Ajouter Java 17 au debut du PATH
set PATH=%JAVA_HOME%\bin;%PATH%

echo.
echo JAVA_HOME defini:
echo %JAVA_HOME%

echo.
echo Version Java active:
java -version

echo.
echo ======================================================================
echo Configuration Java 17 terminee !
echo Vous pouvez maintenant executer les jobs Spark
echo ======================================================================
echo.

REM Garder la fenetre ouverte
cmd /k
