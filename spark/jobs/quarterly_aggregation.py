"""Quarterly aggregation job.

Reads raw events from PostgreSQL, computes features, writes to the analytics
schema. Runs once per quarter (or on demand in demo mode).

TODO (Phase 2):
  - Configure SparkSession with the PostgreSQL JDBC driver
  - Read raw.logistics_events and raw.hrss_telemetry
  - Compute aggregations:
      * Per-asset rolling statistics over the quarter
      * Anomaly rate per asset (from HRSS is_anomalous flag)
      * Energy-efficiency proxy (compare is_optimised vs standard)
  - Write to analytics.* tables with overwrite semantics
  - Ensure idempotency: re-running the same job over the same window
    produces the same output

This file is invoked by Airflow's SparkSubmitOperator. See
airflow/dags/dark_factory_pipeline.py.
"""

from pyspark.sql import SparkSession


def main() -> None:
    raise NotImplementedError(
        "quarterly_aggregation.py is a Phase 2 implementation task. "
        "See module docstring."
    )


if __name__ == "__main__":
    main()
