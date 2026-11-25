# Spark standalone cluster image (master + workers).
# Same Python (3.12) and same pyspark (4.0.1) as the Airflow image,
# because PySpark requires driver and workers to run the same Python version.
FROM python:3.12-slim-trixie

RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==4.0.1 pandas pyarrow

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYSPARK_PYTHON=python3 \
    SPARK_NO_DAEMONIZE=true

WORKDIR /usr/local/airflow
