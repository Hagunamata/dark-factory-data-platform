"""One-shot bulk generator for the initial historical data load.

Loads the source schemas, generates the configured row counts for both
sources (default 600 000 logistics + 400 000 HRSS = 1 000 000 total),
and pushes everything to the configured Kafka topics as JSON messages.

Run via `make seed` once the stack is up.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import numpy as np
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from .generators import generate_hrss_batch, generate_logistics_batch
from .schemas import load_hrss_schema, load_logistics_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("data_generator.bootstrap")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_LOGISTICS = os.environ.get("KAFKA_TOPIC_LOGISTICS", "logistics_events")
TOPIC_HRSS = os.environ.get("KAFKA_TOPIC_HRSS", "hrss_telemetry")

LOGISTICS_ROWS = int(os.environ.get("GENERATOR_LOGISTICS_ROWS", "600000"))
HRSS_ROWS = int(os.environ.get("GENERATOR_HRSS_ROWS", "400000"))
WINDOW_MONTHS = int(os.environ.get("GENERATOR_TIME_WINDOW_MONTHS", "18"))

BATCH_SIZE = int(os.environ.get("GENERATOR_BATCH_SIZE", "25000"))
SEED = int(os.environ.get("GENERATOR_SEED", "42"))


# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------

def make_producer(retries: int = 30, retry_delay_s: float = 2.0) -> KafkaProducer:
    """Wait for Kafka to be reachable, then construct a JSON-serialising producer."""
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Async batching — kafka-python flushes when either limit hits.
                linger_ms=50,
                batch_size=64 * 1024,
                acks=1,
            )
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
            return producer
        except NoBrokersAvailable as e:  # pragma: no cover — env-dependent
            last_err = e
            log.info("Kafka not ready (attempt %d/%d), retrying in %.1fs",
                     attempt, retries, retry_delay_s)
            time.sleep(retry_delay_s)
    raise RuntimeError(f"Kafka unreachable at {KAFKA_BOOTSTRAP}") from last_err


# ---------------------------------------------------------------------------
# Source-level run
# ---------------------------------------------------------------------------

def _run_source(
    name: str,
    topic: str,
    total_rows: int,
    schema,
    generator_fn,
    rng: np.random.Generator,
    producer: KafkaProducer,
    window_start: datetime,
    window_end: datetime,
) -> None:
    log.info("Generating %d %s rows → topic %r in batches of %d",
             total_rows, name, topic, BATCH_SIZE)
    sent = 0
    t0 = time.monotonic()
    while sent < total_rows:
        chunk = min(BATCH_SIZE, total_rows - sent)
        rows = generator_fn(rng, schema, chunk, window_start, window_end)
        for row in rows:
            producer.send(topic, value=row)
        sent += chunk
        elapsed = time.monotonic() - t0
        rate = sent / elapsed if elapsed > 0 else 0.0
        log.info("  %s: %d / %d sent (%.0f rows/s)", name, sent, total_rows, rate)
    producer.flush()
    log.info("%s: done in %.1fs", name, time.monotonic() - t0)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("Loading source schemas from sample_data/")
    logistics_schema = load_logistics_schema()
    hrss_schema = load_hrss_schema()
    log.info("  logistics: %d source rows, %d columns",
             logistics_schema.row_count, len(logistics_schema.columns))
    log.info("  hrss:      %d source rows, %d columns",
             hrss_schema.row_count, len(hrss_schema.columns))

    window_end = datetime.now(tz=timezone.utc)
    window_start = window_end - timedelta(days=30 * WINDOW_MONTHS)
    log.info("Time window: %s → %s (%d months)",
             window_start.isoformat(), window_end.isoformat(), WINDOW_MONTHS)

    rng = np.random.default_rng(SEED)
    producer = make_producer()

    try:
        _run_source(
            "logistics", TOPIC_LOGISTICS, LOGISTICS_ROWS,
            logistics_schema, generate_logistics_batch,
            rng, producer, window_start, window_end,
        )
        _run_source(
            "hrss", TOPIC_HRSS, HRSS_ROWS,
            hrss_schema, generate_hrss_batch,
            rng, producer, window_start, window_end,
        )
    finally:
        producer.close()

    log.info("Bootstrap complete: %d logistics + %d hrss = %d rows total",
             LOGISTICS_ROWS, HRSS_ROWS, LOGISTICS_ROWS + HRSS_ROWS)


if __name__ == "__main__":
    main()
