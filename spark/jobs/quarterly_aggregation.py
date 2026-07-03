"""Quarterly batch aggregation: raw.* → analytics.* (PySpark).

This is the quarterly job referenced in docs/01-conception.md §6:
the ML application re-trains once per quarter and reads only from
`analytics.*`; this job is what makes those tables fresh.

Inputs
------
  raw.logistics_events         (one quarter slice)
  raw.hrss_telemetry           (one quarter slice)

Outputs
-------
  analytics.logistics_features_quarterly   (one row per (quarter, asset_id))
  analytics.hrss_features_quarterly        (one row per (quarter, is_anomalous, is_optimised))

Idempotency
-----------
The job DELETEs any existing rows for the target quarter before writing,
so re-running for the same quarter produces the same end state. The DELETE
runs on the Spark driver via psycopg2 (the driver process lives inside the
Airflow container, which has psycopg2 from airflow/Dockerfile).

Submission
----------
Invoked by Airflow's SparkSubmitOperator (Phase 2 step 5). For ad-hoc runs:

  docker compose exec airflow-webserver spark-submit \\
      --master spark://spark-master:7077 \\
      --packages org.postgresql:postgresql:42.7.3 \\
      /opt/spark/jobs/quarterly_aggregation.py \\
      --quarter-start 2025-01-01
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timezone
from typing import Optional, Tuple

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("spark.quarterly_aggregation")


# ---------------------------------------------------------------------------
# Config (read on the driver — env vars propagated via SparkSubmitOperator)
# ---------------------------------------------------------------------------

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_DB = os.environ.get("POSTGRES_DB", "darkfactory")
PG_USER = os.environ.get("POSTGRES_USER", "darkfactory")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}

LOGISTICS_TABLE = "raw.logistics_events"
HRSS_TABLE = "raw.hrss_telemetry"
LOGISTICS_FEATURES_TABLE = "analytics.logistics_features_quarterly"
HRSS_FEATURES_TABLE = "analytics.hrss_features_quarterly"


# ---------------------------------------------------------------------------
# Quarter math
# ---------------------------------------------------------------------------

def quarter_bounds_for(d: date) -> Tuple[date, date]:
    """Return (start_inclusive, end_exclusive) for the quarter containing `d`.

    Quarters: Jan-Mar, Apr-Jun, Jul-Sep, Oct-Dec.
    """
    q_index = (d.month - 1) // 3                # 0..3
    start_month = q_index * 3 + 1
    start = date(d.year, start_month, 1)
    if q_index == 3:
        end = date(d.year + 1, 1, 1)
    else:
        end = date(d.year, start_month + 3, 1)
    return start, end


def previous_quarter_start(reference: Optional[datetime] = None) -> date:
    """First day of the most recently *completed* quarter relative to `reference`."""
    if reference is None:
        reference = datetime.now(tz=timezone.utc)
    current_start, _ = quarter_bounds_for(reference.date())
    # Step back one day from current_start → into the previous quarter
    prev_day = date.fromordinal(current_start.toordinal() - 1)
    prev_start, _ = quarter_bounds_for(prev_day)
    return prev_start


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("dark-factory-quarterly-aggregation")
        # Single-node scope (conception doc §4.3): keep shuffle parallelism low.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Reads — filter by quarter via a JDBC subquery (predicate pushdown)
# ---------------------------------------------------------------------------

def _jdbc_read_quarter(
    spark: SparkSession,
    table: str,
    timestamp_col: str,
    quarter_start: date,
    quarter_end: date,
) -> DataFrame:
    # Use `query` (not `dbtable`) so the WHERE clause runs on Postgres and
    # only the quarter slice ever reaches Spark. For ~166k rows per quarter
    # on a 2-core worker, a single partition is pragmatic — see
    # conception doc §4.3 ("demonstrates scalability pattern, not absolute scale").
    sql = (
        f"SELECT * FROM {table} "
        f"WHERE {timestamp_col} >= '{quarter_start.isoformat()}' "
        f"  AND {timestamp_col} <  '{quarter_end.isoformat()}'"
    )
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("query", sql)
        .options(**JDBC_PROPS)
        .load()
    )


def read_logistics(spark, qs, qe) -> DataFrame:
    return _jdbc_read_quarter(spark, LOGISTICS_TABLE, "event_timestamp", qs, qe)


def read_hrss(spark, qs, qe) -> DataFrame:
    return _jdbc_read_quarter(spark, HRSS_TABLE, "reading_timestamp", qs, qe)


# ---------------------------------------------------------------------------
# Aggregations — output columns must match analytics.* DDL exactly
# ---------------------------------------------------------------------------

def aggregate_logistics(df: DataFrame, quarter_start: date) -> DataFrame:
    return (
        df.groupBy("asset_id")
          .agg(
              F.count("*").alias("event_count"),
              # delay_rate: fraction of events where logistics_delay = TRUE
              F.avg(F.col("logistics_delay").cast("double")).alias("delay_rate"),
              F.avg("waiting_time_min").alias("avg_waiting_time_min"),
              F.avg("inventory_level").alias("avg_inventory_level"),
              F.avg("asset_utilization_pct").alias("avg_asset_utilization_pct"),
              F.avg("temperature_c").alias("avg_temperature_c"),
              F.avg("humidity_pct").alias("avg_humidity_pct"),
              F.sum("user_transaction_amount").alias("total_transaction_amount"),
          )
          .withColumn("quarter_start", F.lit(quarter_start.isoformat()).cast("date"))
          .select(
              "quarter_start", "asset_id", "event_count",
              "delay_rate", "avg_waiting_time_min", "avg_inventory_level",
              "avg_asset_utilization_pct", "avg_temperature_c", "avg_humidity_pct",
              "total_transaction_amount",
          )
    )


# Per-axis power column names — keep order aligned with the analytics DDL.
_HRSS_POWER_COLS = (
    "blo_power_w", "bhl_power_w", "bhr_power_w",
    "bru_power_w", "hr_power_w",  "hl_power_w",
)


def aggregate_hrss(df: DataFrame, quarter_start: date) -> DataFrame:
    total_power_expr = sum((F.col(c) for c in _HRSS_POWER_COLS), F.lit(0))
    enriched = df.withColumn("total_power_w", total_power_expr)
    return (
        enriched.groupBy("is_anomalous", "is_optimised")
                .agg(
                    F.count("*").alias("reading_count"),
                    F.avg("blo_power_w").alias("avg_blo_power_w"),
                    F.avg("bhl_power_w").alias("avg_bhl_power_w"),
                    F.avg("bhr_power_w").alias("avg_bhr_power_w"),
                    F.avg("bru_power_w").alias("avg_bru_power_w"),
                    F.avg("hr_power_w").alias("avg_hr_power_w"),
                    F.avg("hl_power_w").alias("avg_hl_power_w"),
                    F.sum("total_power_w").alias("total_power_w"),
                    # anomaly_label_rate: fraction of rows where label = 1
                    F.avg(F.col("label").cast("double")).alias("anomaly_label_rate"),
                )
                .withColumn("quarter_start", F.lit(quarter_start.isoformat()).cast("date"))
                .select(
                    "quarter_start", "is_anomalous", "is_optimised", "reading_count",
                    "avg_blo_power_w", "avg_bhl_power_w", "avg_bhr_power_w",
                    "avg_bru_power_w", "avg_hr_power_w",  "avg_hl_power_w",
                    "total_power_w", "anomaly_label_rate",
                )
    )


# ---------------------------------------------------------------------------
# Idempotent write — DELETE quarter, then APPEND fresh rows
# ---------------------------------------------------------------------------

def _delete_quarter(table: str, quarter_start: date) -> int:
    """DELETE existing analytics rows for the given quarter (driver-side).

    Spark JDBC writer has no upsert mode. To stay idempotent we wipe the
    quarter via psycopg2 before appending. Returns number of rows deleted.
    """
    with psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {table} WHERE quarter_start = %s",
                (quarter_start,),
            )
            deleted = cur.rowcount
        conn.commit()
    log.info("Deleted %d existing rows from %s for quarter %s",
             deleted, table, quarter_start)
    return deleted


def _write_features(df: DataFrame, table: str) -> None:
    (df.write
       .format("jdbc")
       .option("url", JDBC_URL)
       .option("dbtable", table)
       .options(**JDBC_PROPS)
       .mode("append")
       .save())


def write_quarter_idempotent(df: DataFrame, table: str, quarter_start: date) -> None:
    _delete_quarter(table, quarter_start)
    row_count = df.count()
    _write_features(df, table)
    log.info("Wrote %d rows to %s for quarter %s", row_count, table, quarter_start)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--quarter-start",
        help="First day of the quarter to aggregate, ISO format (YYYY-MM-DD). "
             "Defaults to the most recently completed quarter relative to now (UTC).",
    )
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)

    if args.quarter_start:
        quarter_start = date.fromisoformat(args.quarter_start)
    else:
        quarter_start = previous_quarter_start()
    quarter_start, quarter_end = quarter_bounds_for(quarter_start)

    log.info("Aggregating quarter %s → %s (exclusive)", quarter_start, quarter_end)

    spark = build_spark()
    try:
        logistics_raw = read_logistics(spark, quarter_start, quarter_end)
        hrss_raw = read_hrss(spark, quarter_start, quarter_end)

        # Pre-count via Spark — cheap and useful as a sanity log line.
        logistics_count = logistics_raw.count()
        hrss_count = hrss_raw.count()
        log.info("Read %d logistics rows and %d HRSS rows for the quarter",
                 logistics_count, hrss_count)

        if logistics_count == 0 and hrss_count == 0:
            log.warning("No rows for quarter %s — nothing to aggregate.", quarter_start)
            return

        logistics_features = aggregate_logistics(logistics_raw, quarter_start)
        hrss_features = aggregate_hrss(hrss_raw, quarter_start)

        write_quarter_idempotent(logistics_features, LOGISTICS_FEATURES_TABLE, quarter_start)
        write_quarter_idempotent(hrss_features, HRSS_FEATURES_TABLE, quarter_start)
    finally:
        spark.stop()

    log.info("Quarter %s done.", quarter_start)


if __name__ == "__main__":
    main()
