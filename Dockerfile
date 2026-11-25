# Airflow (Astro Runtime 3.3 = Airflow 3.3, Debian 13) pinned to Python 3.12
# so the Spark driver (here) and the Spark workers (docker/spark) use the same Python.
FROM astrocrpublic.azurecr.io/runtime:3.3-10-python-3.12

# PostgreSQL JDBC driver used by Spark to load the DWH.
USER root
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark-jars/postgresql-42.7.4.jar
RUN chmod 644 /opt/spark-jars/postgresql-42.7.4.jar
USER astro

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYSPARK_PYTHON=python3
