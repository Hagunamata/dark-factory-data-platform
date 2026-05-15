# Phase 2 — Claude Code Handoff Brief

This document is the bridge between Phase 1 (conception, done in chat) and Phase 2 (development, done with Claude Code). It captures every decision already made so Claude Code can pick up without re-deriving anything.

**Read this first when you open Claude Code in this repo.** Paste a link to it, or copy its contents into your first message, so Claude Code has full context.

---

## Project in one paragraph

A batch-processing data infrastructure for a fully automated dark-factory logistics system, built as a portfolio submission for a project-management-framed Data Engineering course. The course's framing matters: **breadth and reasoning matter more than depth**. Every component should be implemented in its simplest reasonable configuration. The conception document (`docs/01-conception.md`) is the authoritative spec.

## What's already decided

The full reasoning is in `docs/01-conception.md`. The high-level summary:

| Decision | Choice | Why |
|---|---|---|
| Domain | Dark factory logistics | Strong portfolio narrative |
| Mode | Batch (not streaming) | Course track 1.1; ML retrains quarterly |
| Ingestion | Apache Kafka, KRaft mode, single broker | Canonical buffer; decouples producers |
| Raw storage | PostgreSQL, `raw` schema | Pragmatic at this scale |
| Processing | Apache Spark, single-node | Demonstrates scalability pattern |
| Serving | PostgreSQL, `analytics` schema | ELT-into-the-warehouse |
| Orchestration | Apache Airflow, LocalExecutor | Industry standard for scheduled batch |
| Observability | ELK stack | Matches course videos |
| IaC | Docker Compose + Makefile | One-command bring-up |
| Data sources | Two Kaggle datasets (Smart Logistics, HRSS) as schema refs + synthetic generator | Real schemas, controllable scale |
| Volume target | 1M+ rows: ~600k logistics, ~400k HRSS | Meets brief requirement |
| Schedules | Hourly ingest, quarterly Spark (compressible in demo mode) | Matches use case |

## Architecture diagram

`docs/architecture.svg`. Five-layer data path (sources → Kafka → raw Postgres → Spark → analytics Postgres) with two cross-cutting services (Airflow, ELK).

## Repo structure (already created)

```
.
├── README.md
├── docker-compose.yml          ← skeleton, needs completion
├── Makefile                    ← convenience commands, complete
├── .env.example                ← template, complete
├── .gitignore                  ← complete
├── docs/
│   ├── 01-conception.md        ← Phase 1 deliverable, complete
│   ├── architecture.svg        ← complete
│   ├── 02-development.md       ← to be created during Phase 2
│   └── claude-code-handoff.md  ← this file
├── data_generator/
│   ├── README.md
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── __init__.py
│   ├── schemas.py              ← stub
│   ├── distributions.py        ← stub
│   ├── generators.py           ← stub
│   └── bootstrap.py            ← stub
├── airflow/dags/
│   └── dark_factory_pipeline.py  ← stub
├── spark/jobs/
│   └── quarterly_aggregation.py  ← stub
├── postgres/init/
│   └── 01_schemas.sql          ← schemas defined, column lists TODO
├── elk/logstash/
│   └── pipeline.conf           ← stub
└── sample_data/
    └── README.md               ← describes what CSVs the user should download
```

## What "done" looks like for Phase 2

Phase 2 ends when **all of these are true**:

1. `make up` brings the entire stack online with no errors
2. `make seed` generates 1M+ synthetic rows and pushes them to Kafka
3. Airflow's `dark_factory_pipeline` DAG runs end-to-end in demo mode
4. Postgres `raw` schema contains all ingested events
5. Postgres `analytics` schema contains aggregated features after the Spark job
6. Kibana dashboard shows logs flowing in from all services
7. `docs/02-development.md` exists, describing what was built, what went wrong, and what was changed from the Phase 1 plan
8. Screenshots of all the above are saved to `docs/screenshots/` for the portfolio

## Suggested Phase 2 order of work

Build bottom-up. Each step builds on the previous; debugging is easier when only one new thing has been introduced.

### Step 1 — Infrastructure foundation
- Complete `docker-compose.yml` (pin image versions, add healthchecks, init containers for Airflow DB)
- Verify `make up` brings up Postgres, Kafka, and Airflow successfully
- Verify Postgres init script creates both schemas

### Step 2 — Data generator
- Download both Kaggle datasets into `sample_data/`
- Implement `schemas.py` reading the CSVs and extracting column metadata
- Implement `distributions.py` (timestamps, sampling, null injection)
- Implement `generators.py` row generation functions
- Implement `bootstrap.py` to generate 1M+ rows and push to Kafka
- Test that messages actually appear in Kafka topics

### Step 3 — Raw ingestion
- Write the Kafka → Postgres consumer task (an Airflow PythonOperator)
- Verify it bulk-inserts into `raw.*` correctly

### Step 4 — Spark processing
- Configure Spark with the Postgres JDBC driver
- Implement the quarterly aggregation job
- Verify Spark UI shows the job, and `analytics.*` populates

### Step 5 — Airflow DAG wiring
- Convert the stub DAG into a real one with `PythonOperator` and `SparkSubmitOperator`
- Wire up demo-mode schedules (compressed intervals)
- Run the DAG end-to-end via `make demo`

### Step 6 — Observability
- Configure Logstash to pick up container logs
- Verify logs flow into Elasticsearch
- Create a Kibana dashboard (export to `kibana/dashboards/`)

### Step 7 — Portfolio polish
- Take screenshots of: Airflow DAG view, Spark UI, Kibana dashboard, a SQL query against `analytics.*`
- Write `docs/02-development.md` documenting the build
- Tag a release in git: `v0.2-phase2-complete`

## Known sharp edges

These are flagged in conception doc §9. Heads-up so they don't surprise you:

- **Airflow + Spark in Compose.** `SparkSubmitOperator` from inside an Airflow task talking to a separate Spark container needs the Spark binaries on the Airflow image OR a custom airflow image. The Astronomer custom image guide is a good reference. Simpler alternative: have the Airflow task `docker exec` into the Spark master, but that breaks container isolation.
- **Airflow needs its own metadata DB.** Reusing the main Postgres is fine — just use a dedicated schema or database (`CREATE DATABASE airflow`) so it doesn't collide with `raw` and `analytics`.
- **ELK is memory-hungry.** If Elasticsearch keeps OOMing, lower its `ES_JAVA_OPTS` heap or move to Loki+Grafana. The conception doc names this as the fallback.
- **Synthetic data realism.** Don't over-engineer this. Empirical bootstrapping per column with mild jitter is sufficient. Don't burn time on copulas or GAN-based generation.
- **Schema evolution.** If you change `raw.*` table columns mid-Phase-2, you'll need to drop and recreate. `make nuke` does that.

## What NOT to do

These were considered and rejected in Phase 1. Don't reintroduce them:

- ❌ No MinIO / data lake — Postgres `raw` schema is enough at this scale
- ❌ No multi-broker Kafka cluster — single broker, KRaft mode
- ❌ No Spark cluster on Kubernetes — single-node container is the point
- ❌ No CeleryExecutor or KubernetesExecutor for Airflow — LocalExecutor only
- ❌ No custom transformations in pandas — Spark is the processing engine
- ❌ No 3rd Kaggle dataset (robot navigation) — out of scope
- ❌ No streaming pipeline — that's Phase 3 discussion, not implementation
- ❌ No CI/CD pipeline — documented as production extension, not implemented

## Style and discipline

- **Working code over clever code.** This project will be read by a course assessor, not a senior engineer at Google.
- **Comment the why, not the what.** Anyone can see `kafka_producer.send()`. The interesting comment is why we send JSON instead of Avro.
- **Be honest in commits and PRs.** "Tried X, didn't work, fell back to Y because Z" is more valuable for the portfolio than rewriting history.
- **Don't gold-plate.** If a function works, move on. Phase 3 has reflection time built in for cleanup.

## Phase 3 — what's next after Phase 2

Phase 3 is finalisation: reflection document, abstract, and discussion of streaming extension. No new code. The deliverable is `docs/03-finalization.md`. We'll come back to chat for that — Claude Code's job ends when Phase 2 is signed off.

---

## First message to send Claude Code

Once you've opened a Claude Code session in this repo, paste something like:

> I'm working on Phase 2 of a data engineering portfolio project. Read `docs/claude-code-handoff.md`, `docs/01-conception.md`, and `README.md` in that order, then summarise back to me your understanding of the project and what the first three concrete tasks are. Don't write any code yet — I want to confirm we're aligned first.

This gives Claude Code time to load context before doing anything, which produces much better work than diving straight in.
