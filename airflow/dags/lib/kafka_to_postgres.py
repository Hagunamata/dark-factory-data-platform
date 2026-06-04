"""Kafka → Postgres bulk consumer.

Drains a Kafka topic into a `raw.*` Postgres table. Designed to be invoked
by an Airflow PythonOperator on an hourly schedule (per docs/01-conception.md
§6: "Raw ingestion (Kafka → Postgres) — Hourly").

Reliability model
-----------------
- Kafka offsets are committed **manually** and **only after** the Postgres
  insert for a batch succeeds. If the task crashes mid-batch, the
  uncommitted messages are re-delivered on the next run. This makes the
  ingestion at-least-once (with possible duplicates if Postgres committed
  before Kafka did) — acceptable for an append-only `raw` layer.
- Bulk inserts use psycopg2.extras.execute_values for ~100× throughput
  over per-row INSERTs.
- The function stops after `max_idle_polls` consecutive empty polls so the
  Airflow task terminates cleanly when the topic has been fully drained.

Configuration
-------------
All connection parameters can be passed explicitly, or are read from the
environment (see .env.example):
  KAFKA_BOOTSTRAP_SERVERS
  POSTGRES_HOST / POSTGRES_DB / POSTGRES_USER / POSTGRES_PASSWORD
"""

from __future__ import annotations

import json
import logging
import os
from typing import Iterable, List, Optional, Sequence

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from kafka.errors import KafkaError

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Topic → table descriptors
# ---------------------------------------------------------------------------
# Defines, for each source, which Postgres columns to extract from each
# Kafka message. Field order matches the column order so positional binding
# in execute_values is straightforward.

LOGISTICS_COLUMNS: tuple[str, ...] = (
    "event_timestamp",
    "asset_id",
    "latitude",
    "longitude",
    "inventory_level",
    "shipment_status",
    "temperature_c",
    "humidity_pct",
    "traffic_status",
    "waiting_time_min",
    "user_transaction_amount",
    "user_purchase_frequency",
    "logistics_delay_reason",
    "asset_utilization_pct",
    "demand_forecast",
    "logistics_delay",
)

HRSS_COLUMNS: tuple[str, ...] = (
    "reading_timestamp",
    "cycle_elapsed_sec",
    "label",
    "blo_position_mm", "blo_power_w", "blo_voltage_v",
    "bhl_position_mm", "bhl_power_w", "bhl_voltage_v",
    "bhr_position_mm", "bhr_power_w", "bhr_voltage_v",
    "bru_position_mm", "bru_power_w", "bru_voltage_v",
    "hr_position_mm",  "hr_power_w",  "hr_voltage_v",
    "hl_position_mm",  "hl_power_w",  "hl_voltage_v",
    "is_anomalous",
    "is_optimised",
)


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------

def _env_kafka_bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def _env_pg_dsn() -> dict:
    return {
        "host":     os.environ.get("POSTGRES_HOST", "postgres"),
        "port":     int(os.environ.get("POSTGRES_PORT", "5432")),
        "dbname":   os.environ.get("POSTGRES_DB", "darkfactory"),
        "user":     os.environ.get("POSTGRES_USER", "darkfactory"),
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
    }


# ---------------------------------------------------------------------------
# Core consumer
# ---------------------------------------------------------------------------

def consume_topic_to_table(
    topic: str,
    table: str,                                # schema-qualified, e.g. "raw.logistics_events"
    columns: Sequence[str],
    *,
    bootstrap_servers: Optional[str] = None,
    pg_dsn: Optional[dict] = None,
    group_id: Optional[str] = None,
    batch_size: int = 1000,
    poll_timeout_ms: int = 5000,
    max_idle_polls: int = 3,
    max_messages: Optional[int] = None,
) -> int:
    """Drain `topic` into `table`. Returns the number of rows inserted.

    Args:
        topic:             Kafka topic to consume.
        table:             Schema-qualified Postgres table, e.g. "raw.logistics_events".
        columns:           Column names to extract from each JSON message.
                           Missing keys are inserted as NULL.
        bootstrap_servers: Kafka bootstrap (default: $KAFKA_BOOTSTRAP_SERVERS).
        pg_dsn:            psycopg2 connect kwargs (default: from env).
        group_id:          Kafka consumer group (default: "raw-ingest-<table>").
        batch_size:        Rows per Postgres INSERT.
        poll_timeout_ms:   How long to block on each Kafka poll.
        max_idle_polls:    Stop after this many consecutive empty polls.
        max_messages:      Optional cap (useful for tests / demos).

    Returns:
        Number of rows inserted (== number of Kafka messages successfully consumed).
    """
    bootstrap_servers = bootstrap_servers or _env_kafka_bootstrap()
    pg_dsn = pg_dsn or _env_pg_dsn()
    group_id = group_id or f"raw-ingest-{table.replace('.', '-')}"

    insert_sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
    )

    log.info("Starting consumer: topic=%s table=%s group=%s bootstrap=%s",
             topic, table, group_id, bootstrap_servers)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        # Resume from last committed offset; if no commit exists, start from earliest.
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        # Deserialise JSON eagerly; messages with invalid JSON raise here and the
        # batch is not committed, so they will be re-delivered on retry. That's
        # arguably wrong for poison messages, but at this scope keeping it simple.
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # max_poll_records caps how many messages poll() returns at once;
        # we then bulk-insert in batches of `batch_size` (typically equal).
        max_poll_records=batch_size,
        # Consumer client id helps trace which task instance produced which logs.
        client_id=f"airflow-{group_id}",
    )

    conn = psycopg2.connect(**pg_dsn)
    conn.autocommit = False

    total_inserted = 0
    idle_polls = 0

    try:
        while True:
            poll_result = consumer.poll(timeout_ms=poll_timeout_ms)
            if not poll_result:
                idle_polls += 1
                log.info("Empty poll (%d/%d)", idle_polls, max_idle_polls)
                if idle_polls >= max_idle_polls:
                    log.info("No new messages — draining complete.")
                    break
                continue
            idle_polls = 0

            # poll() returns {TopicPartition: [records...]}. Flatten preserving order.
            records = [r for batch in poll_result.values() for r in batch]
            rows = [_extract_row(rec.value, columns) for rec in records]

            _bulk_insert(conn, insert_sql, rows)
            conn.commit()
            consumer.commit()   # commit offsets only after DB commit succeeded
            total_inserted += len(rows)

            log.info("Inserted %d rows (running total: %d)", len(rows), total_inserted)

            if max_messages is not None and total_inserted >= max_messages:
                log.info("Reached max_messages cap (%d).", max_messages)
                break
    except (KafkaError, psycopg2.Error) as e:
        log.exception("Consumer failed; rolling back uncommitted batch: %s", e)
        conn.rollback()
        raise
    finally:
        consumer.close()
        conn.close()

    log.info("Consumer finished: %d rows inserted into %s", total_inserted, table)
    return total_inserted


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_row(message: dict, columns: Sequence[str]) -> tuple:
    """Pull `columns` out of a message dict, defaulting missing keys to None."""
    return tuple(message.get(col) for col in columns)


def _bulk_insert(conn, insert_sql: str, rows: Iterable[tuple]) -> None:
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=1000)


# ---------------------------------------------------------------------------
# Convenience wrappers for the DAG
# ---------------------------------------------------------------------------
# These are what the PythonOperator targets in dark_factory_pipeline.py.
# They wrap consume_topic_to_table with the right topic / table / columns
# and read the topic name from the env (matches .env.example).

def ingest_logistics(**kwargs) -> int:
    return consume_topic_to_table(
        topic=os.environ.get("KAFKA_TOPIC_LOGISTICS", "logistics_events"),
        table="raw.logistics_events",
        columns=LOGISTICS_COLUMNS,
    )


def ingest_hrss(**kwargs) -> int:
    return consume_topic_to_table(
        topic=os.environ.get("KAFKA_TOPIC_HRSS", "hrss_telemetry"),
        table="raw.hrss_telemetry",
        columns=HRSS_COLUMNS,
    )


if __name__ == "__main__":  # pragma: no cover
    # Allows ad-hoc runs from inside the airflow container, e.g.:
    #   docker compose exec airflow-webserver python -m lib.kafka_to_postgres logistics
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    which = sys.argv[1] if len(sys.argv) > 1 else "logistics"
    if which == "logistics":
        ingest_logistics()
    elif which == "hrss":
        ingest_hrss()
    else:
        raise SystemExit(f"Unknown source {which!r}; expected 'logistics' or 'hrss'")
