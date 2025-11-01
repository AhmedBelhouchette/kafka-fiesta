# Dockerfile
FROM apache/spark:latest

# Passer en root pour installer des packages
USER root

# Installer pip et les dépendances Python
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install --no-cache-dir \
    influxdb-client \
    kafka-python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Créer le répertoire de travail
RUN mkdir -p /opt/spark-work-dir

# Copier tout le projet dans le conteneur
COPY . /opt/spark-work-dir/

# Définir le répertoire de travail
WORKDIR /opt/spark-work-dir

# Créer le dossier pour le cache Ivy
RUN mkdir -p /tmp/.ivy2/cache && chmod -R 777 /tmp/.ivy2

# Rester en root (pour éviter les problèmes de permissions)
USER root

# Commande par défaut (sera overridée par docker-compose)
CMD ["/bin/bash"]