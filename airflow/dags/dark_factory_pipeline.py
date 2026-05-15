"""Dark Factory pipeline DAG.

Schedules the two recurring jobs in the platform:
  1. Hourly flush from Kafka topics to the raw.* schema in Postgres
  2. Quarterly Spark batch job that reads raw.* and writes analytics.*

In demo mode (DEMO_MODE=true), schedules are compressed so an end-to-end
cycle completes in minutes instead of months.

TODO (Phase 2):
  - Implement the kafka_to_postgres task (likely a PythonOperator that
    consumes from Kafka and bulk-inserts into raw.*)
  - Implement the spark_aggregation task (SparkSubmitOperator pointing at
    spark/jobs/quarterly_aggregation.py)
  - Wire up sensors so the spark job waits for raw data to be present
  - Add task-level retries and alerting
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dark_factory_pipeline",
    default_args=default_args,
    description="End-to-end batch pipeline for the dark factory data platform",
    schedule="@hourly",          # Will be parameterised via DEMO_MODE in Phase 2
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dark-factory", "batch"],
) as dag:

    # TODO: replace EmptyOperators with real implementations
    ingest = EmptyOperator(task_id="kafka_to_postgres")
    process = EmptyOperator(task_id="spark_aggregation")
    notify = EmptyOperator(task_id="notify_downstream")

    ingest >> process >> notify
