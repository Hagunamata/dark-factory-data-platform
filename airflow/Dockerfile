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

USER airflow

# pyspark gives us spark-submit + the JARs; the providers wire it into Airflow.
RUN pip install --no-cache-dir \
        "pyspark==3.5.3" \
        "apache-airflow-providers-apache-spark==4.8.1" \
        "apache-airflow-providers-postgres==5.11.1" \
        "kafka-python==2.0.2"
