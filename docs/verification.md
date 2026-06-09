# Verification Guide

Step-by-step checks that each Phase 2 component actually works. Run these **in order** — every step assumes the previous ones passed.

Commands work identically on Windows PowerShell and Ubuntu bash unless flagged. On Windows, the only quirks are file paths (no shell-style `cd` needed because every command is `docker compose exec`-ed into a container).

> If a step fails, look at `docs/setup.md` §8 (Troubleshooting) first. Most failures are environment issues, not code bugs.

---

## Step 0 — Bring the stack up

### Run

```bash
docker compose up -d
```

First run takes **5–15 minutes** (image pulls + custom Airflow image build).

### Expected

```bash
docker compose ps
```

Should show **11 services**. All should be either `Up`, `Up (healthy)`, or — only for `airflow-init` — `Exited (0)`:

| Service | Expected status |
|---|---|
| `kafka` | `Up (healthy)` |
| `postgres` | `Up (healthy)` |
| `spark-master` | `Up (healthy)` |
| `spark-worker` | `Up` |
| `airflow-init` | `Exited (0)` |
| `airflow-webserver` | `Up (healthy)` |
| `airflow-scheduler` | `Up` |
| `elasticsearch` | `Up (healthy)` |
| `logstash` | `Up` |
| `kibana` | `Up` |
| `data-generator` | `Up` |

If anything is `Restarting`, check `docker compose logs <service>`.

---

## Step 1 — Infrastructure foundation

### 1a. Postgres has both databases and both schemas

```bash
docker compose exec postgres psql -U darkfactory -l
```

**Expected:** the list contains `darkfactory` AND `airflow`. Both owned by `darkfactory`.

```bash
docker compose exec postgres psql -U darkfactory -d darkfactory -c "\dn"
```

**Expected:** two custom schemas listed: `raw` and `analytics`.

### 1b. Raw and analytics tables exist with the right columns

```bash
docker compose exec postgres psql -U darkfactory -d darkfactory -c "\dt raw.*"
docker compose exec postgres psql -U darkfactory -d darkfactory -c "\dt analytics.*"
```

**Expected:**
- `raw.logistics_events`, `raw.hrss_telemetry`
- `analytics.logistics_features_quarterly`, `analytics.hrss_features_quarterly`

```bash
docker compose exec postgres psql -U darkfactory -d darkfactory -c "\d raw.logistics_events"
```

**Expected** column list (in any order):
```
event_id, event_timestamp, asset_id, latitude, longitude, inventory_level,
shipment_status, temperature_c, humidity_pct, traffic_status, waiting_time_min,
user_transaction_amount, user_purchase_frequency, logistics_delay_reason,
asset_utilization_pct, demand_forecast, logistics_delay, ingested_at
```

### 1c. Kafka cluster is alive

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**Expected:** an empty list (no topics yet), or just `__consumer_offsets`. No errors.

### 1d. Airflow web UI is reachable

Open <http://localhost:8080>. Log in with `airflow` / `airflow`.

**Expected:**
- The `dark_factory_pipeline` DAG appears in the DAG list
- It's **paused** (toggle off — that's the configured default)
- Clicking it opens the graph view showing 3 tasks: `ingest_logistics`, `ingest_hrss`, `spark_quarterly_aggregation`
- No DAG import errors at the top of the page

---

## Step 2 — Data generator

### Run

```bash
docker compose exec data-generator python -m data_generator.bootstrap
```

This takes **3–8 minutes** depending on CPU.

### Expected log output

```
Loading source schemas from sample_data/
  logistics: 1000 source rows, 15 columns
  hrss:      90467 source rows, 21 columns
Time window: 2024-12-... → 2026-06-... (18 months)
Connected to Kafka at kafka:9092
Generating 600000 logistics rows → topic 'logistics_events' in batches of 25000
  logistics: 25000 / 600000 sent (~12000 rows/s)
  logistics: 50000 / 600000 sent (~12500 rows/s)
  ...
  logistics: 600000 / 600000 sent (~12000 rows/s)
logistics: done in 50.0s
Generating 400000 hrss rows → topic 'hrss_telemetry' in batches of 25000
  ...
hrss: done in 35.0s
Bootstrap complete: 600000 logistics + 400000 hrss = 1000000 rows total
```

(Exact row counts depend on `GENERATOR_LOGISTICS_ROWS` / `GENERATOR_HRSS_ROWS` in `.env`. Defaults total 1 000 000.)

### Verify messages reached Kafka

```bash
docker compose exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic logistics_events
```

**Expected:** `logistics_events:0:600000` (offset equals row count).

```bash
docker compose exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic hrss_telemetry
```

**Expected:** `hrss_telemetry:0:400000`.

### Sanity check one message

```bash
docker compose exec kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic logistics_events --from-beginning --max-messages 1
```

**Expected:** a JSON object with the fields:
```
event_timestamp, asset_id, latitude, longitude, inventory_level,
shipment_status, temperature_c, humidity_pct, ...
```

Field names match the `raw.logistics_events` columns from Step 1b.

---

## Step 3 — Raw ingestion (Kafka → Postgres)

The consumer module lives in the Airflow container. You can test it in isolation, without involving the DAG yet.

### Run the logistics consumer directly

```bash
docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-scheduler python -c "from lib.kafka_to_postgres import ingest_logistics; ingest_logistics()"
```

Takes **~30–90 seconds** for 600k rows.

### Expected log output

```
Starting consumer: topic=logistics_events table=raw.logistics_events group=raw-ingest-raw-logistics_events ...
Inserted 1000 rows (running total: 1000)
Inserted 1000 rows (running total: 2000)
...
Empty poll (1/3)
Empty poll (2/3)
Empty poll (3/3)
No new messages — draining complete.
Consumer finished: 600000 rows inserted into raw.logistics_events
```

### Verify rows landed

```bash
docker compose exec postgres psql -U darkfactory -d darkfactory \
    -c "SELECT COUNT(*) FROM raw.logistics_events;"
```

**Expected:** `600000` (or whatever your `GENERATOR_LOGISTICS_ROWS` was).

```bash
docker compose exec postgres psql -U darkfactory -d darkfactory \
    -c "SELECT asset_id, COUNT(*) FROM raw.logistics_events GROUP BY asset_id ORDER BY COUNT(*) DESC LIMIT 5;"
```

**Expected:** `Truck_1` through `Truck_N` with roughly even counts.

### Run the HRSS consumer

```bash
docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-scheduler python -c "from lib.kafka_to_postgres import ingest_hrss; ingest_hrss()"
```

**Expected:** `Consumer finished: 400000 rows inserted into raw.hrss_telemetry`.

```bash
docker compose exec postgres psql -U darkfactory -d darkfactory \
    -c "SELECT is_anomalous, is_optimised, COUNT(*) FROM raw.hrss_telemetry GROUP BY 1,2;"
```

**Expected:** four rows (one per class). Counts should roughly match the original HRSS file proportions (anomalous-optimised ≈ 20%, etc.).

### Idempotency check

Re-run the consumer:

```bash
docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-scheduler python -c "from lib.kafka_to_postgres import ingest_logistics; ingest_logistics()"
```

**Expected:** consumer immediately reports 3 empty polls and exits with `0 rows inserted`. This is the manual-offset-commit behaviour: messages already consumed by this group are not re-delivered.

---

## Step 4 — Spark quarterly aggregation

You need raw data first (Step 3 done). Pick a quarter that contains data — given the generator's 18-month window ending today, any of the last six quarters will work. Use a known one explicitly to make the test deterministic:

```bash
docker compose exec postgres psql -U darkfactory -d darkfactory -c \
  "SELECT date_trunc('quarter', event_timestamp)::date AS q, COUNT(*)
   FROM raw.logistics_events GROUP BY 1 ORDER BY 1;"
```

Pick any quarter with a non-trivial count, e.g. `2025-07-01`.

### Run the job directly

```bash
docker compose exec airflow-scheduler spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.7.3 \
    /opt/spark/jobs/quarterly_aggregation.py \
    --quarter-start 2025-07-01
```

First run downloads the Postgres JDBC JAR from Maven (~30 s extra). Subsequent runs are cached.

### Expected output (excerpts)

```
INFO  spark.quarterly_aggregation: Aggregating quarter 2025-07-01 → 2025-10-01 (exclusive)
INFO  spark.quarterly_aggregation: Read 99876 logistics rows and 66432 HRSS rows for the quarter
INFO  spark.quarterly_aggregation: Deleted 0 existing rows from analytics.logistics_features_quarterly for quarter 2025-07-01
INFO  spark.quarterly_aggregation: Wrote 9 rows to analytics.logistics_features_quarterly for quarter 2025-07-01
INFO  spark.quarterly_aggregation: Deleted 0 existing rows from analytics.hrss_features_quarterly for quarter 2025-07-01
INFO  spark.quarterly_aggregation: Wrote 4 rows to analytics.hrss_features_quarterly for quarter 2025-07-01
INFO  spark.quarterly_aggregation: Quarter 2025-07-01 done.
```

(Exact row counts depend on your data; the **shape** is what matters: ~9 logistics rows (one per truck), exactly 4 HRSS rows (the 2×2 anomalous×optimised matrix).)

### Verify analytics tables

```bash
docker compose exec postgres psql -U darkfactory -d darkfactory \
    -c "SELECT * FROM analytics.logistics_features_quarterly WHERE quarter_start='2025-07-01' ORDER BY asset_id;"
```

**Expected:** one row per `asset_id`, with `event_count`, `delay_rate`, `avg_*` columns populated.

```bash
docker compose exec postgres psql -U darkfactory -d darkfactory \
    -c "SELECT * FROM analytics.hrss_features_quarterly WHERE quarter_start='2025-07-01';"
```

**Expected:** four rows covering `(false,false), (false,true), (true,false), (true,true)`.

### Idempotency check

Re-run the same submit command. Expect `Deleted N existing rows ...` to report the previous count, then re-insert the same rows. Row count in analytics stays the same.

### Spark UI

Open <http://localhost:8081>. You should see:
- Status: `ALIVE`
- One worker registered
- A completed application named `dark-factory-quarterly-aggregation` in the **Completed Applications** list

---

## Step 5 — Airflow DAG end-to-end

This re-runs all three tasks (ingest + Spark) through the orchestrator instead of by hand. Useful to confirm the wiring.

### Reset state so the DAG has work to do

```bash
docker compose exec postgres psql -U darkfactory -d darkfactory \
    -c "TRUNCATE raw.logistics_events, raw.hrss_telemetry, analytics.logistics_features_quarterly, analytics.hrss_features_quarterly;"
```

Reset Kafka consumer group offsets so the ingest tasks have new messages to read (otherwise they'll see "nothing to do"):

```bash
docker compose exec kafka kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group raw-ingest-raw-logistics_events --reset-offsets --to-earliest \
    --topic logistics_events --execute

docker compose exec kafka kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group raw-ingest-raw-hrss_telemetry --reset-offsets --to-earliest \
    --topic hrss_telemetry --execute
```

### Unpause and trigger

In the Airflow UI (<http://localhost:8080>):
1. Toggle the `dark_factory_pipeline` DAG from paused to **on**.
2. Click the **▶ Trigger DAG** button.

Or via CLI:

```bash
docker compose exec airflow-scheduler airflow dags unpause dark_factory_pipeline
docker compose exec airflow-scheduler airflow dags trigger dark_factory_pipeline
```

### Expected

Run finishes in **~5–10 minutes**. In the **Graph view** for the run:
- `ingest_logistics` → **success** (dark green)
- `ingest_hrss` → **success** (dark green)
- `spark_quarterly_aggregation` → **success** (dark green)

Confirm the data made the full trip:

```bash
docker compose exec postgres psql -U darkfactory -d darkfactory -c "
  SELECT 'raw.logistics' AS t, COUNT(*) FROM raw.logistics_events
  UNION ALL SELECT 'raw.hrss', COUNT(*) FROM raw.hrss_telemetry
  UNION ALL SELECT 'analytics.logistics', COUNT(*) FROM analytics.logistics_features_quarterly
  UNION ALL SELECT 'analytics.hrss', COUNT(*) FROM analytics.hrss_features_quarterly;"
```

**Expected:** all four counts non-zero.

### Common DAG-level failure modes

| Symptom | Likely cause |
|---|---|
| `ingest_*` task hangs forever | Kafka consumer group has already drained — see "Reset state" above |
| `spark_*` task fails immediately with `ConnectionRefusedError` | Spark connection misconfigured; check `AIRFLOW_CONN_SPARK_DEFAULT` env in `airflow-scheduler` container |
| `spark_*` task fails with `ClassNotFoundException: org.postgresql.Driver` | First-run JAR download failed (network); retry the task |

---

## Step 6 — Observability (ELK)

### 6a. Logstash is receiving GELF packets

```bash
docker compose logs logstash --tail=20
```

**Expected:** at the bottom, a `[main] Successfully started Logstash` line, no errors about Elasticsearch reachability.

### 6b. Elasticsearch has indices

```bash
curl -s http://localhost:9200/_cat/indices/darkfactory-logs-*?v
```

**Expected:** at least one row, e.g.:
```
health status index                       docs.count  store.size
green  open   darkfactory-logs-2026.06.04      12345      4.2mb
```

`docs.count > 0` is the key signal.

### 6c. Sample a log document

```bash
curl -s "http://localhost:9200/darkfactory-logs-*/_search?size=1&pretty"
```

**Expected:** a hit with at least these fields populated:
- `@timestamp`
- `message`
- `service` (one of `spark-master`, `spark-worker`, `airflow-webserver`, `airflow-scheduler`, `data-generator`)

If `service` is missing, the GELF tag isn't being set — check the `logging:` block on the producing service in `docker-compose.yml`.

### 6d. Kibana shows logs in Discover

1. Open <http://localhost:5601>.
2. **Stack Management → Saved Objects → Import** → upload `kibana/dashboards/00-bootstrap-data-view.ndjson`.
3. Open **Discover** in the left nav.
4. Pick **Dark Factory Logs** as the data view.
5. Set the time range to **Last 15 minutes**.

**Expected:** live log entries appear. The `service` field on the left sidebar shows the five producing services.

### 6e. Build and export the dashboard (one-time owner action)

Follow the workflow in `kibana/dashboards/README.md` — recommended visualisations are listed there. Export the final dashboard as `kibana/dashboards/dark_factory_overview.ndjson` and commit it.

---

## Quick smoke test summary

If you're short on time, this is the minimum sequence that proves everything works:

```bash
docker compose up -d
# wait until: docker compose ps shows everything healthy
docker compose exec data-generator python -m data_generator.bootstrap
docker compose exec airflow-scheduler airflow dags unpause dark_factory_pipeline
docker compose exec airflow-scheduler airflow dags trigger dark_factory_pipeline
# wait until Airflow UI shows the run as success
docker compose exec postgres psql -U darkfactory -d darkfactory -c "
  SELECT COUNT(*) FROM analytics.logistics_features_quarterly;
  SELECT COUNT(*) FROM analytics.hrss_features_quarterly;"
curl -s http://localhost:9200/_cat/indices/darkfactory-logs-*?v
```

If the analytics counts are non-zero and `_cat/indices` returns at least one index, **the pipeline is working end to end.**

---

## Capturing the portfolio screenshots

Once everything above passes, capture the screenshots listed in `docs/screenshots/README.md`. Those are the visual evidence the portfolio reviewer will see.
