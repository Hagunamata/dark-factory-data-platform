# Conception Phase: Batch Data Architecture for a Dark Factory Logistics System

**Course:** Project: Data Engineering
**Phase:** 1 — Conception
**Author:** Hakwoon Chung
**Date:** 15-05-2026

---

## 1. Project context

A *dark factory* is the manufacturing methodology of fully automating the production of goods at factories and other industrial facilities, without requiring any human labour presence on-site (Gisi, 2024). Such a facility produces a continuous stream of operational telemetry: logistics events from the warehouse management system, sensor readings from conveyor belts and high-rack storage systems, and movement data from automated equipment. Each event carries a timestamp, an asset identifier, and a payload describing a measurement or a business action.

The downstream consumer of this data is a machine learning application that re-trains a set of predictive models once per quarter. Typical use cases for such models include predictive maintenance of equipment, anomaly detection across the logistics flow, and energy-optimisation of automated storage cycles. The ML application itself is **out of scope** for this project; the project provides the data infrastructure that prepares and serves the training data.

This problem fits naturally into a **batch processing architecture** for three reasons. First, the ML consumer re-trains on a quarterly cadence, so there is no business need for sub-second latency between event and feature availability. Second, batch processing allows the use of cheaper, simpler, more mature tooling than a streaming equivalent. Third, the engineering effort required to make a batch system reliable is significantly lower than the equivalent for a streaming system. As Kleppmann (2017, p.488 ~ 490) addressed, thanks to the framework, your code in a batch processing job does not need to worry about implementing fault-tolerance mechanisms: the framework can guarantee that the final output of a job is the same as if no faults had occurred, even though in reality various tasks perhaps had to be retried. These reliable semantics are much stronger than what you usually have in online services that handle user requests and that write to databases as a side effect of processing a request.

## 2. Data sources and volume strategy

Two open datasets from Kaggle form the *schema reference* for this project:

- **Smart Logistics Supply Chain Dataset** [https://www.kaggle.com/datasets/ziya07/smart-logistics-supply-chain-dataset] — provides realistic columns for shipment events, asset tracking and delay records. Used as the schema for the **logistics events** source. Approximately 1,000 rows.
- **High-Rack Storage System (HRSS) Sensor Data for Energy Optimisation** [https://www.kaggle.com/datasets/inIT-OWL/high-storage-system-data-for-energy-optimization] — sensor telemetry from a high-rack automated storage system, recorded during retrieval cycles. The dataset is published as four files capturing the cross-product of two operational dimensions (normal vs. anomalous behaviour; standard vs. energy-optimised control), with approximately 20,000 rows per file. Used as the schema for the **automated storage telemetry** source.

The HRSS dataset is a strong domain fit: high-rack automated storage systems are core infrastructure in modern dark factories, and the explicit anomaly and optimisation labels in the source data align directly with two plausible downstream ML use cases — predictive maintenance (detecting anomalous operation before failure) and energy optimisation (learning which control patterns minimise energy use). The four source files are combined into a single source schema with two flag columns (`is_anomalous`, `is_optimised`) preserving the original distinctions.

Neither dataset alone meets the project's 1,000,000-row requirement (Smart Logistics is ~1,000 rows; HRSS is ~80,000 rows combined). Rather than abandon domain-appropriate datasets for a generic 1M-row file, this project implements a **synthetic data generator** that produces 1,000,000+ rows preserving the schemas and statistical properties of the source datasets.

This approach is more representative of real data engineering practice than using a single large public dataset would be. In a real factory deployment, operational data is proprietary and rarely available at scale before a pipeline is built; engineers commonly develop and validate infrastructure against simulated load. The generator therefore reflects a realistic engineering workflow rather than circumventing the requirement.

The generator targets the following data volume:

- ~600,000 logistics event rows spanning an 18-month window
- ~400,000 automated storage telemetry rows spanning the same window
- Realistic timestamp distributions (business-hour weighting, shift-pattern density)
- Plausible value distributions derived from the source datasets' statistical characteristics
- Preservation of the anomaly and optimisation label proportions present in the HRSS source data
- A controlled fraction of missing values and out-of-range readings to mimic real sensor behaviour

The raw 1,000-row Smart Logistics dataset is committed to the repository as a schema reference. The generated 1M+ rows are produced on demand and excluded from version control.

## 3. Architecture overview

The architecture is a five-layer batch pipeline supported by two cross-cutting services. Each layer is implemented as one or more isolated Docker containers communicating over an internal network.

The visual architecture diagram is attached as `docs/architecture.svg`.

**Data path layers (top to bottom):**

1. **Sources** — two Kaggle CSV bundles act as schema references; a Python producer service replays generated rows as discrete events. The two source streams are logistics events (from Smart Logistics) and automated storage telemetry (from HRSS).
2. **Ingestion** — Apache Kafka acts as a durable buffer that decouples producers from consumers.
3. **Raw storage** — PostgreSQL holds an append-only `raw` schema; events are flushed from Kafka hourly.
4. **Processing** — Apache Spark runs a quarterly batch job that reads from `raw`, computes aggregations and engineered features, and writes to the analytics layer.
5. **Serving** — PostgreSQL holds an `analytics` schema with ML-ready feature tables that the downstream ML application queries directly.

**Cross-cutting services:**

- **Apache Airflow** orchestrates the pipeline: scheduling the hourly Kafka-to-raw flush and the quarterly Spark job.
- **ELK stack** (Elasticsearch, Logstash, Kibana) provides centralised logging across all containers and a Kibana dashboard for monitoring.

The shape of this architecture follows established batch-processing reference patterns published by major cloud vendors and standardised in the data engineering community.
The entire stack is defined in a single `docker-compose.yml` so that one command (`make up`) brings the system online. This is the project's expression of *Infrastructure as Code*: the compose file plus a small set of init scripts is sufficient to reproduce the entire environment on any machine with Docker installed.

## 4. Component justifications

Each component below is justified against the question "why this one, and not an alternative?" The course brief asks for this reasoning explicitly, and it is the strongest defence against the criticism that a stack was assembled without thought.

### 4.1 Apache Kafka — ingestion buffer

Kafka acts as a durable landing zone between producers and the rest of the pipeline. Producers write events into Kafka topics; downstream consumers (the hourly flush job) read from those topics at their own pace. This decoupling is the fundamental ingestion pattern in modern data architecture.

**Why Kafka:** Kafka is run as a cluster of one or more servers that can span multiple datacenters or cloud regions. Some of these servers form the storage layer, called the brokers (Kafka.apache.org, 2026).Especially as the canonical open-source ingestion buffer, has wide community support, is heavily documented, and is one of the tools the course explicitly highlights in its tutorials. Its disk-backed log model means events survive a consumer restart without loss.

**Why not alternatives:** A direct write from producers to PostgreSQL would couple producers to the database's availability and write capacity, eliminating the buffer. A file-based landing zone (e.g. S3/MinIO) would work but adds a component without adding capability at this project's scale. RabbitMQ is queue-shaped rather than log-shaped and is less natural for replayable event streams.

**Implementation discipline:** Kafka is run in KRaft mode (no Zookeeper) as a single broker. Default configuration is used. The project does not tune partitioning, replication, or retention beyond defaults — these are documented as production extensions, not implementation work.

### 4.2 PostgreSQL — raw and serving storage

PostgreSQL serves a dual role: it holds the raw event tables and, in a separate schema, the analytics feature tables.

**Why PostgreSQL:** it is mature, free, supports both transactional and analytical workloads at this scale, and is universally understood. Two schemas (`raw` and `analytics`) cleanly separate the lineage of data without requiring a second database technology. This is sometimes called the **ELT-into-the-warehouse** pattern.

**Why not alternatives:** A data lake (MinIO, HDFS) is the canonical raw storage in larger systems but adds a layer this project does not exploit; with ~1M rows, an indexed relational table is faster and simpler. A separate analytical database such as ClickHouse would be over-engineered for the volume.

**Implementation discipline:** Two schemas, four to six tables total. Standard SQL. Connection pooling and credentials are managed via environment variables and Docker secrets pattern.

### 4.3 Apache Spark — batch processing

Spark runs the quarterly aggregation job that transforms raw events into ML-ready features.

**Why Spark:** Apache Spark is a unified analytics engine for large-scale data processing. It provides high-level APIs in Java, Scala, Python and R, and an optimized engine that supports general execution graphs (spark.apache.org). Using Spark in this project directly answers the scalability question: the same PySpark code runs unchanged on a single-node container during development and on a multi-node cluster in production. The choice of Spark is therefore a *demonstration* of an architectural property, not a need driven by current data volume.

**Why not alternatives:** Pure pandas would handle the current data volume on a developer laptop but cannot make any scalability claim. Dask is a credible alternative but is less widely deployed in industry and the course materials lean toward Spark-family tools. SQL inside PostgreSQL alone would handle the aggregations but would not exercise an external processing engine, which the brief implicitly requires.

**Implementation discipline:** a single PySpark job, single-node execution, no custom tuning. The point is to demonstrate the pattern, not to optimise it.

### 4.4 Apache Airflow — orchestration

Airflow schedules and orchestrates the two recurring jobs in the system: the hourly flush from Kafka to the raw schema, and the quarterly Spark aggregation.

**Why Airflow:** orchestration is what makes this system *batch* in the first place — the brief specifies quarterly ML model regeneration, which requires a scheduler that can run a defined DAG at a defined cadence with retries, alerting and observability. Airflow is the industry-standard answer.

**Why not alternatives:** cron is too primitive — no DAG semantics, no retries, no UI. Prefect and Dagster are credible modern alternatives, but Airflow has the broader ecosystem and is more commonly expected in industry.

**Implementation discipline:** a single DAG with three tasks (ingest → process → load) using the LocalExecutor. No Celery, no Kubernetes executor. Default configuration with sensible scheduling.

### 4.5 ELK stack — observability

Logstash collects logs from all containers, Elasticsearch stores them, and Kibana provides a dashboard for browsing and visualising them.

**Why ELK:** observability is part of the brief's reliability and maintainability requirements. Centralised logging across microservices is the minimum bar — without it, debugging a multi-container system means reading individual container logs one by one. ELK is the most widely deployed open-source logging stack and is one of the tools the course materials highlight.

**Why not alternatives:** Prometheus + Grafana is the more modern choice for metrics, but this project focuses on **logs**, not metrics, because logs are the first thing needed when debugging a pipeline. A simpler option (Loki + Grafana) is lighter weight; ELK was chosen because it matches the course's tutorial materials and because the resource budget allows it.

**Implementation discipline:** one Kibana dashboard, one Logstash pipeline parsing container logs, default Elasticsearch settings. No custom parsing rules beyond what is necessary to identify the source service.

### 4.6 Docker Compose — Infrastructure as Code

A single `docker-compose.yml` defines every service, every volume, every network. A `Makefile` provides shortcuts: `make up`, `make seed`, `make down`, `make logs`.

**Why this:** the brief asks for IaC. At this project's scale, a Compose file is the honest answer — it is declarative, version-controlled, and reproducible. Terraform or Pulumi would be appropriate if the target were cloud infrastructure, but the brief specifies local development.

**Why not alternatives:** A bash script that runs docker commands in sequence is procedural, fragile, and not declarative. Kubernetes (minikube, kind) would be over-engineered for a local development stack and adds significant complexity for no learning gain at this scope.

## 5. Cross-cutting concerns

The brief asks how reliability, scalability, maintainability, security, governance and protection are addressed. There is unfortunately no easy fix for making applications reliable, scalable, or maintainable. However, there are certain patterns and techniques that keep reappearing in different kinds of applications. (Kleppmann, 2017, p.38 ~ 41). This project distinguishes between what is **implemented** and what is **documented as production extensions** — an honest separation, given the local development scope.

### 5.1 Reliability

**Implemented:** Kafka provides durable, disk-backed buffering, so a downstream failure does not lose events. Airflow retries failed tasks automatically. Docker health checks and `restart: unless-stopped` policies recover failed containers. The pipeline is **idempotent** — re-running the same Spark job over the same input produces the same output, so retries are safe.

**Production extensions:** Kafka replication factor 3 across multiple brokers, Postgres streaming replication, multi-AZ Spark cluster, alerting via PagerDuty or equivalent, backup-and-restore tested regularly.

### 5.2 Scalability

**Implemented:** The architecture demonstrates *horizontal* scalability *patterns* rather than absolute scale. Kafka can be partitioned across brokers; Spark workers scale linearly; Postgres can be replaced with a distributed equivalent for the analytics layer (e.g. Snowflake, BigQuery) without changing the producer or processing code.

**Production extensions:** Kafka multi-broker cluster with partitioned topics; Spark cluster on YARN or Kubernetes; separation of raw and analytics into different database technologies optimised for each workload.

### 5.3 Maintainability

**Implemented:** Each component is isolated in its own container — failure modes are localised, components can be upgraded independently. Code is version-controlled in a public GitHub repository. The system is reproducible on any machine via a single command. Configuration lives in environment files, not code. The data pipeline is documented in code (the Airflow DAG is self-documenting).

**Production extensions:** CI/CD pipelines, automated testing of DAGs, schema validation for raw events, data quality tests on analytics tables (e.g. Great Expectations), staging and production environment parity.

### 5.4 Data security

**Implemented:** Database credentials are managed through environment variables loaded from a `.env` file that is excluded from version control. Inter-container communication occurs on a private Docker network with no exposed ports outside what is necessary for development. The repository contains no real personal data.

**Production extensions:** TLS between all services; secrets managed via Vault or AWS Secrets Manager rather than env files; per-service service accounts with least-privilege database roles; network segmentation; encryption at rest for Postgres and Elasticsearch volumes; audit logging.

### 5.5 Data governance

**Implemented:** Two schemas (`raw` and `analytics`) establish a clear data lineage — every analytics row is traceable to its raw events. Source datasets are documented in this conception document and in the repository README. Data generation logic is version-controlled.

**Production extensions:** A data catalogue (e.g. DataHub or Amundsen) tracking schemas and ownership; column-level lineage tracking; data retention policies; access control by role; PII tagging.

### 5.6 Data protection

**Implemented:** The synthetic data contains no personal information by construction. Database volumes are persisted within Docker, isolated from the host filesystem. The repository excludes generated data.

**Production extensions:** GDPR-style right-to-erasure tooling; pseudonymisation pipelines for any sensitive fields; backup and disaster-recovery procedures; data masking in non-production environments.

## 6. Schedule and frequencies

The brief asks at what frequency the system ingests, processes, aggregates and delivers data. The cadence below reflects a realistic dark-factory deployment serving a quarterly-retrained ML model.

| Stage | Frequency | Mechanism |
|---|---|---|
| Event production | Continuous (simulated) | Python producer writes to Kafka topics |
| Raw ingestion (Kafka → Postgres) | Hourly | Airflow DAG triggers a consumer that flushes Kafka topics to the `raw` schema |
| Batch processing | Quarterly | Airflow DAG triggers a Spark job that reads `raw`, computes features, writes to `analytics` |
| Delivery to ML application | On-demand | ML application queries the `analytics` schema directly via SQL |

For development purposes, the hourly and quarterly schedules can be compressed (e.g. every five minutes and every hour respectively) so that an end-to-end demo runs in minutes rather than months. The Airflow DAG configuration exposes these intervals as parameters.

## 7. Reproducibility

The project follows the principle that any reasonably configured developer machine should be able to bring up the full stack with a single command. Concretely:

1. `git clone` the repository
2. `cp .env.example .env` and review values
3. `make up` — brings up all containers
4. `make seed` — generates synthetic data and runs the initial historical load
5. `make demo` — runs a compressed end-to-end pipeline cycle

The repository contains:

- `docker-compose.yml` — full service definitions
- `Makefile` — convenience commands
- `data_generator/` — synthetic data generation module
- `airflow/dags/` — orchestration DAGs
- `spark/jobs/` — PySpark aggregation job
- `postgres/init/` — schema initialisation SQL
- `kibana/dashboards/` — exported observability dashboard
- `docs/` — this conception document, the architecture diagram, and per-phase notes

## 8. Advantages and disadvantages of the design

### Advantages

- **Clear component boundaries.** Each service has one job. This makes failures easy to diagnose and components easy to replace.
- **Honest scope.** The implementation does not pretend to be production-grade; it implements canonical patterns in their simplest form and documents what would change at scale. This is more credible than over-engineering.
- **Reproducible by anyone.** A single `make up` command brings the stack online. This satisfies the IaC requirement and makes the project portfolio-friendly.
- **Schema-grounded synthetic data.** Using Kaggle datasets as schema references avoids the trap of fully-invented data that doesn't match any real domain.
- **Quarterly cadence matches the use case.** The architecture is tuned for the ML application's actual rhythm, not for arbitrary "real-time" goals it does not need.

### Disadvantages

- **Synthetic data caveat.** A reviewer may legitimately push back on synthetic data. The mitigation is the schema-reference approach and explicit documentation of the trade-off.
- **No real cluster deployment.** Spark, Kafka and Airflow all run in single-node containers. The system *demonstrates* scalability patterns but does not *prove* them at scale. Cloud deployment is a future extension.
- **Limited fault injection.** The reliability story is sound on paper but is not stress-tested. Chaos engineering (deliberately killing containers and verifying recovery) is a future addition.
- **Two PostgreSQL schemas, one engine.** Using the same database for raw and analytics is pragmatic at this scale but couples their availability. In production, the raw store would likely move to object storage and the analytics store to a columnar warehouse.
- **ELK stack is heavyweight.** Elasticsearch alone uses significant memory. The 32 GB development machine handles it comfortably; a smaller laptop would struggle.

## 9. Risks and open questions

These are tracked openly to inform Phase 2 (development) and Phase 3 (finalisation).

- **Synthetic data realism.** Generating 1M+ rows that *feel* like real factory data requires care with timestamp distributions and value correlations. The Phase 2 plan includes a focused effort here.
- **Airflow + Spark integration on Compose.** Running Spark from within an Airflow task in Docker Compose has known sharp edges. The plan is to use `SparkSubmitOperator` against a containerised Spark, with the executor connection configured explicitly.
- **ELK memory footprint.** If the stack consumes more memory than expected, the fallback is Loki + Grafana, which provides equivalent log aggregation at a fraction of the memory cost.
- **Schema evolution.** If the raw event format changes mid-project (e.g. a new field is added to logistics events), the system must handle it. Phase 2 will implement basic schema validation at the Kafka-consumer step.

## 10. Next phase

Phase 2 (Development) will:

1. Create the GitHub repository with the structure defined above
2. Implement the synthetic data generator
3. Bring up the Docker Compose stack
4. Implement the Airflow DAGs and the Spark job
5. Verify an end-to-end run with the compressed-time demo
6. Capture screenshots and a brief video walkthrough for the portfolio

Phase 2 development will be performed using Claude Code as a pair-programming assistant. The code lives in a public GitHub repository which itself forms part of the portfolio submission alongside this document.

---

## References

Gisi, P. (2024). The Dark Factory and the Future of Manufacturing: A Guide to Operational Efficiency and Competitiveness. *New York, NY: Routledge, Taylor & Francis Group.* p. 3. doi:10.4324/9781032688152

Kafka.apache.org. (2026) Introduction. https://kafka.apache.org/42/getting-started/introduction/

Kleppmann, M. (2017). Designing data-intensive applications: the big ideas behind reliable, scalable, and maintainable systems. *Heidelberg O'Reilly*. 2017 1st Edition. p. 38 ~ 41, 488 ~ 490

Spark.apache.org. Apache Spark - A Unified engine for large-scale data analytics. https://spark.apache.org/docs/latest/
