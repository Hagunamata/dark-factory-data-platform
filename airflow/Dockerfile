# =============================================================================
# Custom Airflow image — adds Java + pyspark so SparkSubmitOperator can
# launch jobs against the spark-master container over spark://.
# Flagged in docs/claude-code-handoff.md as a known sharp edge.
# =============================================================================

FROM apache/airflow:2.9.3-python3.11

USER root

# OpenJDK 17 is required by Spark 3.5.x.
# procps is needed by spark-submit's process management.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Postgres JDBC driver — baked into the image so SparkSubmitOperator does
# NOT need Maven Central reachable from the running container. The DAG
# passes this path via `jars=` instead of `packages=`. Catches corp-proxy
# environments where Ivy's runtime download fails with DNS errors.
RUN mkdir -p /opt/spark/extra-jars && \
    curl -fsSL -o /opt/spark/extra-jars/postgresql-42.7.3.jar \
        https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    chmod 644 /opt/spark/extra-jars/postgresql-42.7.3.jar

USER airflow

# pyspark gives us spark-submit + the JARs; the providers wire it into Airflow.
RUN pip install --no-cache-dir \
        "pyspark==3.5.3" \
        "apache-airflow-providers-apache-spark==4.8.1" \
        "apache-airflow-providers-postgres==5.11.1" \
        "kafka-python==2.0.2"
