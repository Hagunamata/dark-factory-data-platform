# Dark Factory Data Platform

A batch-processing data infrastructure for a fully automated (dark factory) logistics system. Built as the portfolio project for the *Project: Data Engineering* course.

This repository contains the data infrastructure that ingests, stores, processes, and serves operational telemetry from a simulated dark factory to a downstream machine learning application that re-trains predictive models on a quarterly cadence. The ML application itself is out of scope; this project is the data engineering backend.

## At a glance

```
Sources  →  Kafka  →  Postgres (raw)  →  Spark  →  Postgres (analytics)  →  ML application
                            ▲                           │
                            └───── Airflow ─────────────┘
                                        ELK (logs)
```

Five-layer batch pipeline, two cross-cutting services, all wired together by Docker Compose. See `docs/architecture.svg` for the full architecture diagram and `docs/01-conception.md` for the design reasoning.

## Tech stack

| Layer | Component | Why |
|---|---|---|
| Ingestion | Apache Kafka | Durable buffer between producers and the pipeline |
| Raw storage | PostgreSQL (`raw` schema) | Append-only event tables |
| Processing | Apache Spark | Quarterly batch aggregation |
| Serving storage | PostgreSQL (`analytics` schema) | ML-ready feature tables |
| Orchestration | Apache Airflow | Schedules the recurring jobs |
| Observability | ELK stack | Centralised container logs |
| IaC | Docker Compose + Makefile | One command brings the stack online |

Full justification for each choice is in `docs/01-conception.md` section 4.

## Quick start

> [!NOTE]
> The stack runs on Linux or WSL2 with Docker installed. Tested on Ubuntu 22.04 with Docker Engine and Docker Compose v2.

```bash
git clone <this-repo>
cd dark-factory-data-platform
cp .env.example .env       # review and adjust if needed
make up                    # bring up all containers
make seed                  # generate synthetic data and load it
make demo                  # run a compressed end-to-end pipeline cycle
```

Service UIs after `make up`:

| Service | URL | Default credentials |
|---|---|---|
| Airflow | http://localhost:8080 | airflow / airflow |
| Kibana | http://localhost:5601 | none |
| Spark master UI | http://localhost:8081 | none |
| Postgres | localhost:5432 | see `.env` |

## Project structure

```
.
├── README.md                  # this file
├── docs/
│   ├── 01-conception.md       # phase 1 deliverable: design reasoning
│   ├── 02-development.md      # phase 2 deliverable: implementation notes
│   ├── 03-finalization.md     # phase 3 deliverable: reflection and abstract
│   └── architecture.svg       # the architecture diagram
├── docker-compose.yml         # full service definitions
├── Makefile                   # convenience commands
├── .env.example               # template for environment variables
├── .gitignore
├── data_generator/            # synthetic data generation module
├── producers/                 # services that emit events to Kafka
├── airflow/dags/              # orchestration DAGs
├── spark/jobs/                # PySpark aggregation jobs
├── postgres/init/             # schema initialisation SQL
├── kafka/                     # Kafka-related configs
├── elk/logstash/              # Logstash pipeline configs
├── kibana/dashboards/         # exported Kibana dashboards
└── sample_data/               # the original 1k-row Kaggle CSV (schema reference)
```

## Course context

This project is the portfolio submission for a project-management-framed Data Engineering course, delivered across three phases:

1. **Conception** — design decisions, component choices, architecture diagram. Deliverable: `docs/01-conception.md`.
2. **Development** — implementation of the stack and verification of an end-to-end run. Deliverable: this repository in a working state, plus `docs/02-development.md`.
3. **Finalization** — reflection, abstract, and discussion of how the system would be extended to streaming. Deliverable: `docs/03-finalization.md`.

The implementation prioritises **architectural clarity over production hardening**. Every component is run in its simplest reasonable configuration (single broker, single-node executor, default settings) and the conception document documents what would change at production scale. The goal is to demonstrate understanding of canonical patterns, not to ship a production system.

## License

This project is for educational use. The two reference datasets retain their original Kaggle licenses; see `sample_data/README.md` for details.
