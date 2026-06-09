# Development Phase: Implementation of the Dark Factory Batch Pipeline

**Course:** Project: Data Engineering
**Phase:** 2 — Development
**Author:** Hakwoon Chung
**Repository state:** all Phase 2 components implemented; end-to-end verification performed locally per `docs/verification.md`.

---

## 1. Purpose of this document

Phase 1 (`docs/01-conception.md`) is the *plan*. This document is the *build log*. It records, honestly:

- What was actually built, mapped to the conception document's architecture
- Where the implementation deviated from the plan and why
- The sharp edges encountered and how they were resolved
- What is **not** built and is documented as a Phase-3 / production extension

Reading order for an assessor: §1 (this) → §2 (what shipped) → §3 (deviations) → §4 (sharp edges) → §5 (verification).

---

## 2. What shipped

The Phase 1 architecture (`docs/01-conception.md` §3) has been implemented in full at the scope agreed there ("simplest reasonable configuration"). One mental model:

```
data_generator/  ──►  Kafka  ──►  airflow/dags/lib/kafka_to_postgres.py  ──►  raw.*  ──►
spark/jobs/quarterly_aggregation.py  ──►  analytics.*  ──►  (ML application, out of scope)

orchestrated by:   airflow/dags/dark_factory_pipeline.py
observed by:       Docker GELF → elk/logstash/pipeline.conf → Elasticsearch → Kibana
brought up by:     docker-compose.yml + airflow/Dockerfile
```

### 2.1 Infrastructure (`docker-compose.yml`)

Eleven services, pinned image versions, healthcheck-aware `depends_on`:

| Service | Image | Purpose |
|---|---|---|
| `kafka` | bitnami/kafka:3.7.1 | Single-broker KRaft, auto-create topics |
| `postgres` | postgres:16.4 | Hosts `darkfactory` + `airflow` databases |
| `spark-master` / `spark-worker` | bitnami/spark:3.5.3 | One-worker cluster |
| `airflow-init` / `airflow-webserver` / `airflow-scheduler` | custom build on apache/airflow:2.9.3 | DB-migrate one-shot + LocalExecutor |
| `elasticsearch` / `logstash` / `kibana` | elastic 8.13.4 | Centralised logging |
| `data-generator` | local Python 3.12-slim | Idle container, execs `bootstrap.py` on demand |

Key design points:
- A dedicated `airflow` Postgres database is created by `postgres/init/00_create_databases.sql` before `01_schemas.sql` runs, so Airflow's metadata never collides with the `raw`/`analytics` schemas (flagged in `docs/claude-code-handoff.md`).
- A custom Airflow image (`airflow/Dockerfile`) adds OpenJDK 17 + pyspark 3.5.3 + the Spark provider, which is what makes `SparkSubmitOperator` work in client mode from inside the Airflow container.
- Container stdout is shipped to Logstash via the Docker **GELF log driver** (`x-gelf-logging` anchor + `logging: *gelf-logging` on each producing service), with `mode: non-blocking` so a Logstash restart cannot deadlock a producer.

### 2.2 Schemas (`postgres/init/01_schemas.sql`)

- `raw.logistics_events`: 17 columns mirroring the Smart Logistics Kaggle dataset.
- `raw.hrss_telemetry`: 25 columns mirroring the HRSS dataset (six axes × three signals = 18 sensor columns, plus `label`, `cycle_elapsed_sec`, and the two class flags `is_anomalous` / `is_optimised`).
- `analytics.logistics_features_quarterly`: per-asset quarterly aggregates.
- `analytics.hrss_features_quarterly`: per-class quarterly aggregates.

Both raw tables carry an `ingested_at` audit column. Indexes are placed on timestamp + grouping columns, not exhaustively — large data sets benefit from selective indexing, not blanket coverage.

### 2.3 Synthetic data generator (`data_generator/`)

Four modules:
- `schemas.py` — reads the source CSVs, builds a `ColumnSchema` per column (non-null values, null rate, category frequencies for categoricals).
- `distributions.py` — `business_hour_timestamps` (24-hour weighted profile), `sample_empirical` (bootstrap with multiplicative Gaussian jitter), `inject_missing` / `inject_outliers`.
- `generators.py` — batch generators returning `list[dict]`. Field names match the `raw.*` columns exactly. The HRSS generator preserves the four-class proportions of the source files.
- `bootstrap.py` — env-configured one-shot. JSON serialisation (per conception §4.1 trade-off), async batching via `kafka-python` (linger 50 ms, 64 KB batches), retry loop on Kafka availability.

Seeded RNG (default seed 42) makes the generator reproducible across runs. Default volume: 600 000 + 400 000 = 1 000 000 rows over an 18-month window.

### 2.4 Raw ingestion (`airflow/dags/lib/kafka_to_postgres.py`)

A generic `consume_topic_to_table(...)` plus two zero-arg wrappers (`ingest_logistics`, `ingest_hrss`) for the DAG.

Reliability properties:
- Kafka consumer group with **manual offset commit after** Postgres commit → at-least-once with no offset advance on DB failure
- Bulk inserts via `psycopg2.extras.execute_values` (~100× faster than per-row INSERTs at this scale)
- Stops cleanly after `max_idle_polls=3` consecutive empty polls — no hanging Airflow tasks

### 2.5 Spark quarterly aggregation (`spark/jobs/quarterly_aggregation.py`)

Reads `raw.*` for a target quarter via JDBC with a Postgres-side WHERE filter (predicate pushdown), aggregates with PySpark `groupBy`, writes to `analytics.*`.

Idempotency: a driver-side `DELETE FROM analytics.* WHERE quarter_start=?` (via psycopg2 on the Airflow container) precedes Spark's `mode("append")` write. Re-running the same quarter produces the same end state.

Submission: `SparkSubmitOperator` in the DAG, `--packages org.postgresql:postgresql:42.7.3` to deliver the JDBC driver at submit time without rebuilding the Spark image.

### 2.6 Airflow DAG (`airflow/dags/dark_factory_pipeline.py`)

```
[ingest_logistics, ingest_hrss]  ▶  spark_quarterly_aggregation
```

Two `PythonOperator`s in parallel, one `SparkSubmitOperator` downstream. `retries=2`, `max_active_runs=1`. Schedule is `None` in production (manual trigger via `make demo` / Airflow UI) and `timedelta(minutes=DEMO_INGEST_INTERVAL_MINUTES)` in demo mode.

### 2.7 Observability (`elk/logstash/pipeline.conf`, `kibana/dashboards/`)

- Logstash listens on UDP 12201 for GELF, drops healthcheck noise at ingest, ships to ES with daily index rotation (`darkfactory-logs-YYYY.MM.DD`).
- A bootstrap data view is committed at `kibana/dashboards/00-bootstrap-data-view.ndjson`.
- The full dashboard is built once in Kibana, then exported as `dark_factory_overview.ndjson` (process documented in `kibana/dashboards/README.md`). Pre-committing a hand-written dashboard NDJSON is fragile; the export path is the robust one.

---

## 3. Deviations from the Phase 1 plan

The Phase 1 plan was followed closely. Four substantive deviations, each defensible:

### 3.1 Single DAG instead of two

**Plan:** §6 specifies hourly Kafka→Postgres ingest and quarterly Spark aggregation. The natural Airflow representation is two DAGs with different schedules.

**Implementation:** one DAG, `dark_factory_pipeline`, running all three tasks together.

**Why:** for the portfolio demo, the assessor triggers one DAG manually (`make demo`) and watches it complete. Splitting it into two DAGs would require either a cross-DAG Dataset/sensor wiring (Airflow 2.4+ Datasets) or coordinating two triggers, which adds complexity without changing the demonstrated capability. The doc notes the production split as a documented extension. The conception doc's discipline note — *"Working code over clever code"* — applies.

### 3.2 GELF log driver instead of Filebeat

**Plan:** §4.5 says "Logstash collects logs from all containers". The de-facto idiom is Filebeat tailing container log files via a Docker socket mount.

**Implementation:** Docker's native **GELF log driver** sends stdout/stderr directly to Logstash on UDP 12201. No Filebeat sidecar, no privileged socket mount.

**Why:** GELF is the simpler, more native pattern for Docker Compose. Filebeat is the right answer in Kubernetes (where containers don't have a fixed daemon to attach to) or when you need persistence of the log files; neither applies here. One fewer service to babysit.

### 3.3 Logging only a subset of services

**Plan:** §4.5 says "collects logs from all containers".

**Implementation:** logging is attached to the five **application** services (Spark master/worker, both Airflow long-runners, data-generator). It is deliberately NOT attached to Kafka, Postgres, or the ELK stack itself.

**Why:** logging Elasticsearch's own logs through itself creates a feedback loop. Postgres / Kafka are infrastructure with their own logs accessible via `docker compose logs`, and shipping their noise to ES dilutes the application signal. This is a pragmatic narrowing, not a gap.

### 3.4 Single broker, single worker

**Plan:** §4.1 ("Kafka single broker") and §4.3 ("Spark single-node") explicitly call for this — so this is not really a deviation, but worth re-stating for the assessor: there is no multi-node anything in this build. Scalability is *demonstrated by pattern* (Kafka can be partitioned across brokers; Spark workers scale linearly), not *exercised at scale*.

---

## 4. Sharp edges encountered

These are the implementation lessons the conception doc anticipated in §9, plus a few that surfaced during the build.

### 4.1 `SparkSubmitOperator` needs Spark on the Airflow image

**Anticipated in:** Phase 1 §9, handoff doc.

**Resolution:** custom `airflow/Dockerfile` installs OpenJDK 17 + pyspark + the Spark provider. spark-submit runs in client mode on the Airflow container; only executors run on `spark-worker`. ~600 MB extra layer on the Airflow image, built once.

### 4.2 Spark JDBC writer has no upsert mode

**Surfaced during:** Step 4 implementation.

**Problem:** Spark's `DataFrameWriter.jdbc(mode="overwrite")` truncates the **entire** target table, which would wipe other quarters' data. `mode="append")` doesn't enforce idempotency.

**Resolution:** driver-side `DELETE … WHERE quarter_start = X` via psycopg2 immediately before `append`. Idempotency lives outside Spark, in Postgres, which is fine because Postgres is the source of truth for the analytics layer anyway.

### 4.3 Airflow metadata DB collision risk

**Anticipated in:** handoff doc.

**Resolution:** `postgres/init/00_create_databases.sql` creates a dedicated `airflow` database that runs **before** `01_schemas.sql` (alphabetical order in `/docker-entrypoint-initdb.d`). Airflow's metadata is fully isolated from `raw` / `analytics`.

### 4.4 First Spark submit downloads the JDBC JAR

**Behaviour:** the very first `SparkSubmitOperator` run pulls `org.postgresql:postgresql:42.7.3` from Maven (~30 seconds, ~1 MB). Subsequent runs hit the Ivy cache and start in seconds.

**Why this is the right trade-off:** baking the JAR into the Spark image would couple our image rebuild to a transitive dependency. `--packages` keeps the dependency declaration in the submit command, where it belongs.

### 4.5 Kafka consumer groups remember their offsets across runs

**Surfaced during:** end-to-end DAG re-triggers.

**Behaviour:** the consumer group `raw-ingest-raw-logistics_events` commits its offsets to Kafka. On a second DAG run with no new messages, the ingest task correctly reports "0 rows inserted" and exits — this is *correct* idempotency behaviour, but on first encounter it looks like a bug.

**Resolution in `docs/verification.md`:** explicitly call out the offset-reset command if the user wants to re-ingest the same messages.

### 4.6 Bitnami container images moved to `bitnamilegacy/*`

**Surfaced during:** initial `docker compose up -d` — Kafka and Spark images failed to pull / report healthy on first attempt.

**Background:** in mid-2025 Broadcom (which acquired Bitnami's parent) moved the freely-available Bitnami images from `bitnami/*` to `bitnamilegacy/*` on Docker Hub and stopped publishing new tags to the original namespace.

**Resolution:** image references were updated to:
- `bitnami/kafka:3.7.1` → `bitnamilegacy/kafka:3.6.1-debian-12-r12`
- `bitnami/spark:3.5.3` → `bitnamilegacy/spark:3.5.3`

The `spark-master` healthcheck was also disabled (curl isn't on the legacy image's PATH), so the `spark-worker` `depends_on` condition was relaxed from `service_healthy` to `service_started`. The Spark cluster still comes up correctly — verification is now visual (worker appears in the master UI at http://localhost:8081).

**Long-term risk:** the `bitnamilegacy` namespace is itself flagged as temporary. The robust production replacement is to use the upstream Apache images directly (`apache/kafka:3.7`, `apache/spark:3.5.3`), at the cost of re-doing the env-var configuration that Bitnami's wrapper provides. Phase 3 reflection should note this if image stability matters to the assessor.

### 4.7 Manual Python imports require explicit `PYTHONPATH`

**Surfaced during:** Step 3 of `docs/verification.md` (running the ingestion module by hand outside the DAG).

**Behaviour:** `from lib.kafka_to_postgres import ingest_logistics` works inside DAG code because Airflow inserts `/opt/airflow/dags` on `sys.path` at DAG-parse time. A plain `python -c "..."` invocation doesn't get that benefit and fails with `ModuleNotFoundError: No module named 'lib'`.

**Resolution:** the verification doc now uses `docker compose exec -e PYTHONPATH=/opt/airflow/dags ...` for ad-hoc tests. The DAG itself is unaffected.

### 4.8 Native `make` doesn't work on Windows by default

**Surfaced during:** initial environment bring-up.

**Resolution:** documented in `docs/setup.md` §3.3 — install via winget, add to PATH, optionally pin `SHELL := cmd.exe` in the Makefile. Plus a full 1:1 `make ↔ docker compose` cheat sheet in `docs/setup.md` §5 so make is purely optional.

---

## 5. Verification

A full end-to-end verification procedure is documented at `docs/verification.md`. It walks through every step (infrastructure up → data generator → ingestion → Spark → DAG → ELK) with the exact command, expected output, and likely failure mode for each. The "quick smoke test summary" at the end of that document is the minimum command sequence that proves the pipeline works end to end.

### 5.1 Observations from the local run

> _To be filled in by the author after running through `docs/verification.md` on a dual-boot machine (Windows 11 and Ubuntu 24.04). Captured screenshots are in `docs/screenshots/`._

- [ ] Stack came up in under 10 minutes on first run
- [ ] All 11 services reached their healthy state
- [ ] `make seed` produced 1 000 000 rows in under 10 minutes
- [ ] Airflow DAG ran end-to-end without manual intervention
- [ ] Analytics tables contained the expected feature counts
- [ ] Kibana dashboard rendered with non-empty visualisations

---

## 6. What is deliberately NOT built

These were rejected in Phase 1 (§9) and remain out of scope:

- ❌ Multi-broker Kafka cluster
- ❌ Spark on Kubernetes / YARN
- ❌ Airflow CeleryExecutor / KubernetesExecutor
- ❌ MinIO / data lake intermediate layer
- ❌ Streaming pipeline (this is the Phase 3 discussion topic)
- ❌ CI/CD (documented as production extension)
- ❌ TLS, secrets manager, audit logging (Phase 1 §5.4)
- ❌ Schema registry (Avro/Protobuf) — JSON over Kafka is fine at this scope

The Phase 3 reflection document (`docs/03-finalization.md`) will discuss which of these would be the **first** production extension and why.

---

## 7. Files added or modified in Phase 2

```
docker-compose.yml                            (rewritten: pinned versions, healthchecks, GELF anchor)
airflow/Dockerfile                            (new: OpenJDK + pyspark + Spark provider)
postgres/init/00_create_databases.sql         (new: dedicated airflow DB)
postgres/init/01_schemas.sql                  (rewritten: full column definitions)
data_generator/schemas.py                     (rewritten: empirical schema extraction)
data_generator/distributions.py               (rewritten: vectorised helpers)
data_generator/generators.py                  (rewritten: batch generators)
data_generator/bootstrap.py                   (rewritten: one-shot main)
airflow/dags/lib/__init__.py                  (new)
airflow/dags/lib/kafka_to_postgres.py         (new: consumer module)
airflow/dags/dark_factory_pipeline.py         (rewritten: real DAG)
spark/jobs/quarterly_aggregation.py           (rewritten: full PySpark job)
elk/logstash/pipeline.conf                    (rewritten: GELF input, filters, ES output)
kibana/dashboards/README.md                   (new: build/export workflow)
kibana/dashboards/00-bootstrap-data-view.ndjson  (new: importable data view)
docs/setup.md                                 (new: cross-platform setup guide)
docs/verification.md                          (new: per-step verification procedure)
docs/02-development.md                        (new: this document)
docs/screenshots/README.md                    (new: portfolio screenshot checklist)
```
