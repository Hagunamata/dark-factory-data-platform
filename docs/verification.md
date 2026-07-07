# Verification Guide

Reproduces the end-to-end pipeline on a fresh machine and confirms each layer produced the expected result. Commands are identical on Linux and Windows (all commands run through `docker compose exec`).

Prerequisites: the stack is up and healthy (see `docs/setup.md`). Screenshots corresponding to each step live in `docs/screenshots/`.

---

## Step 0 — Stack is up

```bash
docker compose ps
```

All eleven services should report `Up` or `Up (healthy)`; `airflow-init` should report `Exited (0)` (it is a one-shot).

Reference: `docs/screenshots/Docker_ps.png`.

---

## Step 1 — Databases and schemas exist

```bash
docker compose exec postgres psql -U darkfactory -l
```

Contains both `darkfactory` and `airflow` databases.

```bash
docker compose exec postgres psql -U darkfactory -d darkfactory -c "\dn"
```

Lists the `raw` and `analytics` schemas.

```bash
docker compose exec postgres psql -U darkfactory -d darkfactory -c "\dt raw.*; \dt analytics.*"
```

Two `raw.*` and two `analytics.*` tables listed.

---

## Step 2 — Data generator seeds 1M rows

```bash
docker compose exec data-generator python -m data_generator.bootstrap
```

Runs for a few minutes and finishes with:

```
Bootstrap complete: 600000 logistics + 400000 hrss = 1000000 rows total
```

Verify the messages reached Kafka:

```bash
docker compose exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic logistics_events
docker compose exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic hrss_telemetry
```

Offsets sum to 600 000 and 400 000 respectively.

Reference: `docs/screenshots/Data_Generator_1M.png`.

---

## Step 3 — DAG triggered from the Airflow UI

Open `http://localhost:8080` (login `airflow` / `airflow`). Unpause `dark_factory_pipeline` and trigger it.

The DAG contains three tasks: `ingest_logistics` and `ingest_hrss` in parallel, then `spark_quarterly_aggregation`. All three should finish green within a few minutes.

Reference: `docs/screenshots/Airflow_Pipeline_Graph.png`.

Confirm the ingest tasks populated the raw tables:

```bash
docker compose exec postgres psql -U darkfactory -d darkfactory -c "
  SELECT 'raw.logistics' AS t, COUNT(*) FROM raw.logistics_events
  UNION ALL SELECT 'raw.hrss', COUNT(*) FROM raw.hrss_telemetry;"
```

Both counts non-zero.

---

## Step 4 — Spark job populated the analytics tables

The `spark_quarterly_aggregation` task ran inside the DAG (Step 3). The Spark master UI at `http://localhost:8081` shows the completed application; the worker is registered.

Verify the analytics tables:

```bash
docker compose exec postgres psql -U darkfactory -d darkfactory -c \
  "SELECT * FROM analytics.logistics_features_quarterly LIMIT 10;"
docker compose exec postgres psql -U darkfactory -d darkfactory -c \
  "SELECT * FROM analytics.hrss_features_quarterly;"
```

The logistics table has one row per `asset_id` for the target quarter. The HRSS table has four rows covering the `(is_anomalous, is_optimised)` matrix.

Re-running the same quarter is idempotent: the job DELETEs and re-inserts the same rows, so the row count in analytics stays constant.

References: `docs/screenshots/Spark_Quarterly_Aggregation.png`, `Spark_Quarterly_Aggregation_Successful.png`, `Spark_Analytics_Table.png`.

---

## Step 5 — Logs flow through the ELK stack

Logstash is receiving GELF packets and shipping them to Elasticsearch:

```bash
docker compose logs logstash --tail=20
```

Contains `Successfully started Logstash` with no repeated `Elasticsearch Unreachable` warnings.

Reference: `docs/screenshots/Logstash_6a.png`.

Elasticsearch has non-empty daily indices:

```bash
curl -s http://localhost:9200/_cat/indices/darkfactory-logs-*?v
```

At least one row, `docs.count` > 0.

Reference: `docs/screenshots/Elasticsearch_6b.png`.

Sample document has `@timestamp`, `message`, and `service` populated:

```bash
curl -s "http://localhost:9200/darkfactory-logs-*/_search?size=1&pretty"
```

Reference: `docs/screenshots/Sanity_check_6c.png`.

Kibana Discover, at `http://localhost:5601` after importing `kibana/dashboards/00-bootstrap-data-view.ndjson`, shows live log entries with the `service` field distributing across the five instrumented services.

Reference: `docs/screenshots/Elastic_Logs_6d.png`, `Log volume over time.png`.
