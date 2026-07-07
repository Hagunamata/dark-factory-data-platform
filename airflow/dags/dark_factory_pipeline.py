"""Dark Factory pipeline DAG.

    ┌─ ingest_logistics ─┐
    │                    ├──► spark_quarterly_aggregation
    └─ ingest_hrss ──────┘

Both ingest tasks drain their Kafka topics into raw.* in parallel; the
Spark task aggregates the raw data into analytics.* once both finish.

Schedule: when DEMO_MODE=true, runs every DEMO_INGEST_INTERVAL_MINUTES
minutes; otherwise the DAG has no schedule and is triggered manually.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# /opt/airflow/dags is on sys.path inside the container, so `lib.X` resolves
# to airflow/dags/lib/X.py on the host.
from lib.kafka_to_postgres import ingest_hrss, ingest_logistics


# ---------------------------------------------------------------------------
# Schedule (demo-mode aware)
# ---------------------------------------------------------------------------

DEMO_MODE = os.environ.get("DEMO_MODE", "false").lower() == "true"
DEMO_INGEST_INTERVAL_MIN = int(os.environ.get("DEMO_INGEST_INTERVAL_MINUTES", "5"))

SCHEDULE = timedelta(minutes=DEMO_INGEST_INTERVAL_MIN) if DEMO_MODE else None


# ---------------------------------------------------------------------------
# Operator config
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Postgres env passed through to the Spark driver. spark-submit forks a new
# process so it doesn't inherit Airflow's env by default — we hand it
# explicitly so quarterly_aggregation.py can reach Postgres.
SPARK_ENV = {
    "POSTGRES_HOST":     os.environ.get("POSTGRES_HOST", "postgres"),
    "POSTGRES_PORT":     os.environ.get("POSTGRES_PORT", "5432"),
    "POSTGRES_DB":       os.environ.get("POSTGRES_DB", "darkfactory"),
    "POSTGRES_USER":     os.environ.get("POSTGRES_USER", "darkfactory"),
    "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
}


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="dark_factory_pipeline",
    description="End-to-end batch pipeline: Kafka → raw.* → Spark → analytics.*",
    default_args=DEFAULT_ARGS,
    schedule=SCHEDULE,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dark-factory", "phase-2"],
) as dag:

    ingest_logistics_task = PythonOperator(
        task_id="ingest_logistics",
        python_callable=ingest_logistics,
        doc_md="Drain Kafka `logistics_events` → `raw.logistics_events`.",
    )

    ingest_hrss_task = PythonOperator(
        task_id="ingest_hrss",
        python_callable=ingest_hrss,
        doc_md="Drain Kafka `hrss_telemetry` → `raw.hrss_telemetry`.",
    )

    spark_aggregate = SparkSubmitOperator(
        task_id="spark_quarterly_aggregation",
        # Path inside the airflow container (mounted from ./spark/jobs).
        application="/opt/spark/jobs/quarterly_aggregation.py",
        # Connection set via AIRFLOW_CONN_SPARK_DEFAULT in docker-compose.yml.
        conn_id="spark_default",
        # Postgres JDBC driver — baked into airflow/Dockerfile so submit-time
        # Maven downloads are not required (avoids breakage in offline /
        # corporate-proxy environments).
        jars="/opt/spark/extra-jars/postgresql-42.7.3.jar",
        env_vars=SPARK_ENV,
        verbose=False,
        doc_md=(
            "PySpark job: read the latest quarter from raw.*, compute "
            "per-asset and per-class aggregates, write to analytics.*. "
            "Idempotent (DELETE quarter then INSERT)."
        ),
    )

    [ingest_logistics_task, ingest_hrss_task] >> spark_aggregate
