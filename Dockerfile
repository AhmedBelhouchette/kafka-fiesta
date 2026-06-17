# Single image used by every Spark and Python service in docker-compose.
# Base: slim Python + a JRE (Spark needs Java). pyspark's pip wheel bundles a
# full Spark distribution, which we symlink to /opt/spark so the compose
# spark-master/worker commands (/opt/spark/bin/spark-class ...) work unchanged.
# Pinned to bookworm (Debian 12): it still ships openjdk-17, which Spark 3.5
# officially supports. The rolling `slim` tag moved to Trixie, which dropped 17.
FROM python:3.10-slim-bookworm

# Java runtime + procps (Spark launch scripts use `ps`)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# Spark 3.5 on JDK 17 needs these module openings (applied to every JVM here,
# including the standalone master/worker launched via spark-class).
ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED"
WORKDIR /app

# Python deps first for better layer caching
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Expose pyspark's bundled Spark at the path the compose commands expect
RUN ln -s "$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')" /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# App code. PYTHONPATH=/app/src lets jobs import `common.config`, etc.
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1
COPY . /app
