# Development Phase: Implementing the Dark Factory Batch Pipeline

**Course:** Project: Data Engineering
**Phase:** 2 — Development
**Author:** Hakwoon Chung

---

## 1. What this document is

Phase 1 (`docs/01-conception.md`) is the design. This document records what got built, what changed, and what broke.

The end state is a working stack: `docker compose up -d` brings up eleven services, `docker compose exec data-generator python -m data_generator.bootstrap` produces 1 000 000 rows across two Kafka topics, an Airflow DAG drains both topics into `raw.*`, a PySpark job aggregates the quarter into `analytics.*`, and container logs land in Elasticsearch. Screenshots of each step are in `docs/screenshots/`.

## 2. What shipped

The architecture is exactly the one described in `01-conception.md` §3:

```
data_generator/  →  Kafka  →  Kafka→Postgres consumer (Airflow PythonOperator)  →  raw.*
                                                                                    │
                                                                                    ▼
                            analytics.*  ←  quarterly_aggregation.py (Spark)  ←  raw.*
                                                                                    ▲
                                                          orchestration: Airflow DAG
                                                          logs: containers → Logstash → ES → Kibana
```

Concrete implementation choices:

- **Infrastructure** — one `docker-compose.yml` with eleven services, pinned image versions, healthchecks on the stateful ones (Kafka, Postgres, Elasticsearch, Airflow webserver), a dedicated `airflow` database created by `postgres/init/00_create_databases.sql` before the domain schemas so the metadata does not collide with `raw`/`analytics`.
- **Custom Airflow image** (`airflow/Dockerfile`) adds OpenJDK 17, pyspark 3.5.3, and the Spark provider so `SparkSubmitOperator` can submit in client mode from the Airflow container.
- **Data generator** (`data_generator/`) reads the two Kaggle CSVs, builds per-column empirical distributions (values, null rate, category frequencies), and produces JSON rows into Kafka in batches of 25 000 with a seeded RNG so runs are reproducible.
- **Ingestion consumer** (`airflow/dags/lib/kafka_to_postgres.py`) uses a Kafka consumer group with manual offset commits after the Postgres bulk insert (`psycopg2.extras.execute_values`) succeeds. At-least-once is acceptable here because the raw layer is append-only.
- **Spark job** (`spark/jobs/quarterly_aggregation.py`) reads a single quarter via a JDBC subquery so the WHERE filter runs on Postgres, aggregates with `groupBy`, and writes to `analytics.*` idempotently: DELETEs the target quarter's rows first, then appends the fresh ones (Spark's JDBC writer has no upsert).
- **DAG** (`airflow/dags/dark_factory_pipeline.py`) — two `PythonOperator`s in parallel followed by one `SparkSubmitOperator`. `retries=2`, `max_active_runs=1`.
- **Observability** — Docker's GELF log driver ships stdout from the five application services to Logstash on UDP 12201. Logstash promotes the `tag` field to `service` and drops healthcheck noise. Elasticsearch indexes daily (`darkfactory-logs-YYYY.MM.DD`). Kibana reads the `darkfactory-logs-*` data view.

## 3. What I did differently from the Phase 1 plan

Three decisions in Phase 2 diverged from what Phase 1 sketched:

**One DAG instead of two.** The plan called for hourly ingest and quarterly Spark, which naturally splits into two DAGs on different schedules. I kept them in one DAG because the demo trigger is a single `make demo` / manual click, and cross-DAG dependencies via Airflow Datasets would add wiring without changing what the demo shows. Splitting into two DAGs is documented as a production extension.

**GELF log driver instead of Filebeat.** The plan just said "Logstash collects logs from all containers". The textbook implementation is Filebeat tailing container files via a Docker socket mount. GELF is the Docker-native equivalent: no sidecar, no privileged socket, just a `logging:` block per service. Fewer moving parts.

**Logs from application services only.** I attach GELF logging to Spark, Airflow, and the data-generator, but not to Kafka, Postgres, or the ELK stack itself. Sending Elasticsearch's own logs through Elasticsearch creates a feedback loop, and Postgres/Kafka produce a lot of low-value noise that would swamp the app logs. Their logs are still available via `docker compose logs`.

## 4. What broke, and how I fixed it

Three problems dominated the debugging. All three are worth documenting because they are the kind of thing a reader would hit on a fresh machine.

### 4.1 Bitnami container images moved to `bitnamilegacy/*`

Broadcom (which now owns Bitnami) moved the freely-available Bitnami images from `bitnami/*` to `bitnamilegacy/*` on Docker Hub and stopped publishing new tags to the original namespace. My initial `bitnami/kafka:3.7.1` and `bitnami/spark:3.5.3` references failed to pull.

Fix: changed the image references to `bitnamilegacy/kafka:3.6.1-debian-12-r12` and `bitnamilegacy/spark:3.5.3`. The Kafka minor version drop from 3.7 → 3.6 was compatible with everything else in the stack. The Spark master healthcheck also had to be disabled because the legacy image doesn't include `curl` on its PATH, and the `spark-worker` `depends_on` clause was relaxed from `service_healthy` to `service_started`.

### 4.2 Postgres JDBC driver — three attempts before it worked

The Spark job needs the Postgres JDBC JAR to read from `raw.*`. I tried three approaches:

1. **`--packages org.postgresql:postgresql:42.7.3`** — spark-submit's built-in Ivy resolver. Clean, no extra files. Broke immediately with `Host repo1.maven.org not found`: the container network can pull Docker images but not reach Maven Central. Corporate-proxy / split-DNS environment.
2. **Bake the JAR into the Airflow image via `RUN curl ...`** — moves the download to build time, when the host network is available. Broke because either the build cached an earlier layer or `jdbc.postgresql.org` sits behind the same proxy rules; either way, spark-submit reported `Local jar ... does not exist`.
3. **Bind-mount the JAR from the host** — download once to `airflow/jars/postgresql-42.7.3.jar` on the host, mount it read-only into the Airflow container at `/opt/spark/extra-jars/`. This works with no runtime network access, and dropping in another driver later is just adding another file.

Option 3 is what shipped. The story is in `airflow/jars/README.md`.

### 4.3 Spark master URL

After the JDBC driver was in place, the DAG-triggered Spark job still failed to connect to the master. The `SparkSubmitOperator` had `conn_id="spark_default"` and I was setting `AIRFLOW_CONN_SPARK_DEFAULT="spark://spark-master:7077"` as an env var. That works for command-line submits but the SparkSubmit hook parses the connection differently than a plain env var, and I never fully nailed the format it wanted. I ended up making the Spark master URL explicit in `spark/jobs/quarterly_aggregation.py`'s `SparkSession.builder.master(...)` call so the driver doesn't rely on the env-var connection at all. The DAG still passes it, but the job now works even if the connection is missing.

## 5. Verification

Everything above was verified end-to-end. The exact commands and expected outputs are in `docs/verification.md`, and the screenshots are in `docs/screenshots/`.

Key observations from the local run:

- All eleven services reached a healthy state within a few minutes of the first-time image pull.
- The data generator produced 1 000 000 rows across the two Kafka topics in a few minutes on a laptop-class machine.
- The DAG completed end-to-end with all three tasks green. `analytics.logistics_features_quarterly` was populated with one row per truck for the target quarter, and `analytics.hrss_features_quarterly` with four rows covering the `(is_anomalous, is_optimised)` matrix.
- Elasticsearch received log events from all five instrumented services; Kibana Discover shows the `service` field distributing across `spark-master`, `spark-worker`, `airflow-webserver`, `airflow-scheduler`, and `data-generator`.

## 6. What was intentionally left out

Everything the Phase 1 §9 "considered and rejected" list ruled out is still absent: no multi-broker Kafka, no Spark cluster on Kubernetes, no CeleryExecutor, no data lake layer, no streaming pipeline, no CI/CD, no TLS between services. These belong to Phase 3 discussion or to a production extension; the project's scope is single-node correctness and clarity, not scale.
