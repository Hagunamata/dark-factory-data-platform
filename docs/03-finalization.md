# Finalization Phase: Reflection on the Dark Factory Batch Pipeline

**Course:** Project: Data Engineering (DLMDSEDE02)
**Phase:** 3 — Finalization
**Author:** Hakwoon Chung
**Matriculation No.:** IU14094574

---

## 1. What this document is

This is the final product of the three-phase portfolio project. Phase 1 (`docs/01-conception.md`) laid out the design; Phase 2 (`docs/02-development.md`) recorded the implementation and what broke along the way; this document reflects on the outcome, addresses the finalization questions posed in the assignment brief, and describes what would come next. The two-page abstract submitted alongside this document (`Chung_Hakwoon_IU14094574_DLMDSEDE02_Task1_Phase3.pdf`) is a condensed summary of the same material.

The end product is a reproducible batch data infrastructure for a fully automated (dark factory) logistics system. Eleven containerized microservices — Kafka, PostgreSQL, Spark, Airflow, and the ELK stack — cooperate to ingest, store, process and serve one million synthetic telemetry rows to a downstream quarterly machine learning workload. The entire stack is brought online on any machine with Docker installed via a single `docker compose up` command.

**GitHub repository:** (https://github.com/Hagunamata/dark-factory-data-platform)

## 2. Does the system fulfil the technical requirements?

Here are the requirements, fulfilled in this project:

- **A batch-processing data infrastructure** with at least one million time-referenced data points. The synthetic data generator produces 600,000 logistics event rows and 400,000 HRSS telemetry rows, each carrying a timestamp and an asset identifier, spanning an 18-month window. Even though the real world data was limited, implemented data generator could achieve the number of data points, required in the project. 
- **Microservices architecture with Docker containerisation.** Eleven services, each in its own container, communicating over an internal Docker network with pinned image versions and healthchecks on the stateful ones (Kafka, Postgres, Elasticsearch, Airflow webserver).
- **Infrastructure as Code.** A single `docker-compose.yml`, a `Makefile` for convenience commands, and version-controlled schema initialisation scripts. The environment is reproducible on any machine that has Docker installed.
- **Ingestion, storage, pre-processing, aggregation and delivery.** Kafka handles ingestion; PostgreSQL holds both raw and analytics schemas; a PySpark job performs the quarterly aggregation; the analytics tables are queryable by the downstream ML application via standard SQL.
- **Scheduled processing.** An Airflow DAG orchestrates the pipeline, drainable hourly and aggregating quarterly (compressible for demo purposes).

Verification steps are documented in `docs/verification.md` and screenshots of each stage are in `docs/screenshots/`.

## 3. What went wrong, and why

Three implementation issues dominated the debugging effort. All three are documented in more detail in `docs/02-development.md` §4.

**Bitnami image migration.** Broadcom, the current owner of Bitnami, migrated the freely-available Bitnami container images from the `bitnami/*` namespace on Docker Hub to `bitnamilegacy/*` and stopped publishing new tags to the original namespace. My initial `docker-compose.yml` referenced `bitnami/kafka:3.7.1` and `bitnami/spark:3.5.3` and simply failed to pull them. The fix was to pin `bitnamilegacy/kafka:3.6.1-debian-12-r12` and `bitnamilegacy/spark:3.5.3`, disable the Spark master healthcheck (the legacy image lacks `curl` on its PATH), and relax the `spark-worker` `depends_on` clause from `service_healthy` to `service_started`. This is the kind of external change that no amount of planning could have anticipated; the lesson is that dependency management for public container images is a real risk that production systems typically address with private registries.

**The Postgres JDBC driver.** The Spark job needs the Postgres JDBC JAR to read from `raw.*`. Three approaches were attempted before one worked: `--packages` resolution via spark-submit failed because the container network cannot reach Maven Central; baking the JAR into the Airflow image via `RUN curl` failed similarly; and finally, bind-mounting the JAR from the host filesystem into the container worked reliably. This is a good example of how the perfect can be the enemy of the good — the "clean" solution (declarative package resolution) failed in a real environment, and the "primitive" solution (a JAR file on disk) shipped. The lesson is to lead with the simplest approach that works, even if it feels less elegant.

**Spark master URL routing.** After the JDBC driver was in place, the DAG-triggered Spark job still failed to connect to the Spark master. The `SparkSubmitOperator` uses an Airflow connection to determine the master URL, and the format expected by that hook did not match what I set via environment variable. Rather than continue debugging the connection format, I made the master URL explicit in the `SparkSession.builder.master(...)` call inside the job itself. The DAG still passes it, but the job now works even if the Airflow connection is missing. The lesson is that redundancy in configuration is sometimes worth its small cost in duplication.

## 4. Reliability, scalability, maintainability

The abstract addresses this briefly; here is a fuller treatment.

**Reliability.** The system implements four concrete reliability patterns. First, Kafka's disk-backed log ensures that events survive a downstream failure — a Postgres restart, for example, does not lose in-flight events, because they persist in the Kafka log until the consumer commits its offset after a successful insert. Second, the Airflow DAG has `retries=2` on all tasks, so transient failures (network hiccups, Postgres momentarily unavailable) are retried automatically. Third, the Spark job is idempotent: re-running it over the same time window produces the same output, because it deletes the target quarter's rows before inserting the fresh ones. Fourth, Docker restart policies ensure that failed containers come back up automatically.

At production scale, this baseline would extend to: Kafka replication factor 3 across at least three brokers on different failure domains; Postgres streaming replication with a hot standby; multi-AZ Spark clusters; alerting via PagerDuty or equivalent; and backup and disaster-recovery procedures tested regularly.

**Scalability.** This is where the current implementation is explicitly a *pattern*, not a *proof*. The same PySpark code runs unchanged from a single-node container to a multi-node cluster on YARN or Kubernetes; Kafka topics can be partitioned across brokers without touching producer or consumer code; the Postgres analytics schema can be replaced with a columnar warehouse (Snowflake, BigQuery, Redshift) without changing the downstream ML application. The one component that would need attention at real scale is the ingestion consumer — currently a Python script running as an Airflow task, which can be improved as a dedicated Kafka Connect deployment in the real production.

**Maintainability.** Three properties support this. First, each component is isolated in its own container; failure modes are localized and components can be upgraded independently. Second, the entire environment is described as code in a single `docker-compose.yml`, so anyone with Docker can bring up an identical stack in minutes. Third, the code is version-controlled and organized by responsibility: the Airflow DAG describes *what* runs and *when*, the data generator handles *event production*, the Spark job handles *transformation*, and each concern is decoupled.

## 5. Additional measures for data security, governance and protection

The current implementation covers a baseline: database credentials are managed via environment variables loaded from a `.env` file excluded from version control; inter-container communication occurs on a private Docker network; and the synthetic dataset contains no personal information by construction.

For a production deployment, the following extensions would be appropriate:

- **Security:** TLS between all services (currently plaintext within the Docker network); secrets managed via AWS Secrets Manager rather than environment files; per-service database roles with least-privilege permissions; network segmentation between application tiers; encryption at rest for Postgres and Elasticsearch volumes; audit logging of all administrative actions.
- **Governance:** A data catalogue such as DataHub or Amundsen tracking schemas, ownership, and column-level lineage; data retention policies with automated enforcement; role-based access control; PII tagging on any sensitive fields; a formal data quality framework (Great Expectations, for example) with tests on both raw and analytics tables.
- **Protection:** GDPR-style right-to-erasure tooling; pseudonymisation pipelines for any sensitive fields; backup and disaster-recovery procedures tested regularly; data masking in non-production environments; separation of production and development data.

These are documented as extensions as a learning during the implementation, considering the scope of the project.

## 6. How to introduce a real-time streaming pipeline

The most natural extension to this system is a second, streaming pipeline that runs alongside the existing batch one. The two would coexist in a **Lambda architecture**: the batch layer remains the authoritative source of truth, while the streaming layer provides fresh, approximate views for use cases that need low latency.

**What stays.** Kafka is already the ingestion buffer. Every event produced by the data generator is already in a Kafka topic. The streaming pipeline reads from the same topics — no changes needed to producers, no additional infrastructure at the ingestion layer.

**What is added.** A streaming processor consumes the topics in a distinct consumer group. Two candidates are natural:

- **Apache Flink** provides the strongest streaming semantics (event-time processing, exactly-once state management, sophisticated windowing) and is the industry-standard tool for genuinely low-latency processing.
- **Spark Structured Streaming** reuses the existing Spark cluster and the same PySpark code style used in the batch job. This is the pragmatic choice, since operational familiarity and code reuse outweigh Flink's technical advantages at this project's scale.

The streaming processor applies windowed aggregations — tumbling windows for periodic counts (e.g. "shipments per hour"), sliding windows for rolling metrics (e.g. "conveyor anomaly rate over the past 10 minutes") — and writes to a low-latency serving store. Elasticsearch, which is already deployed for logging, could double as this store; a real-time Kibana dashboard would surface the results directly.

**What changes.** Airflow's role narrows. In a pure batch system, Airflow orchestrates everything. In a Lambda topology, Airflow orchestrates only the batch reconciliation jobs (nightly rebuilds of the "correct" view) and historical reprocessing, while streaming runs continuously without an orchestrator.

**Trade-offs.** Streaming systems are meaningfully harder to operate than batch systems. Fault tolerance in streaming requires careful state management; late-arriving data requires watermarks; testing is significantly harder because there is no discrete "input" to feed a test against. The abstract of the Phase 1 conception document noted this explicitly as one reason for choosing batch as the starting point. The streaming extension is a natural next step, but it is a step, not a substitute.

## 7. Improvements to workflow in the next project

Three concrete workflow improvements would meaningfully help a future project:

**Start with a smaller end-to-end slice.** Even at the reduced scale this project targets, because of lack of knowledge on each component, I spent time on individual components (getting the data generator working perfectly, tuning Spark, refining schemas) before verifying the full pipeline ran end-to-end. A more disciplined workflow would build a **degenerate end-to-end slice first** — one row of data flowing through every layer, from producer to Kafka to Postgres to Spark to analytics — and only then flesh out each layer. This "walking skeleton" approach surfaces integration issues early, when they are cheap to fix.

**Fixture data alongside code.** The synthetic data generator is powerful but slow when iterating on the Spark job. A future project would benefit from a small, hand-curated fixture dataset (a few hundred rows) committed alongside the code, so that the Spark job can be tested against known-good input in seconds without regenerating the full million rows.

**A running experiment log.** Debugging the JDBC driver issue took me through three approaches over a couple of days. If I had kept a running log of what I tried and why each attempt failed, the third attempt would have been quicker to design. Designing a log during the development is not easy but saves a lot of time for debugging and helpful for the customer delivery also.

## 8. Major steps and skills gained

The project followed the three-phase structure prescribed by the assignment, each with concrete intermediate deliverables. The major implementation steps within Phase 2 were: composing the eleven-service stack; implementing the synthetic data generator against the two Kaggle schemas; getting the Kafka-to-Postgres ingestion path working end-to-end; getting the Spark job to read from and write to Postgres; wiring everything into an Airflow DAG; and configuring the ELK logging layer.

### Three technical skills

- **Containerized microservice orchestration.** Writing a multi-service Docker Compose file with pinned image versions, healthchecks, private networking, volume management and dependency ordering. This is the foundation on which everything else in modern data engineering sits.
- **Data engineering plumbing.** Designing schemas that cleanly separate raw and analytics layers, and implementing transformations that are *idempotent by construction* even when the underlying storage does not support upserts natively. This is the discipline that makes batch pipelines reliable.
- **Orchestration and observability.** Using Airflow to schedule and monitor recurring jobs, and the ELK stack to centralize logs across all services via Docker's GELF driver. Observability in particular is often treated as an afterthought, but it is the difference between a system that can be operated and one that can be broken on the other day.

### Three soft skills

- **Scope discipline.** The Phase 1 conception document included an explicit "considered and rejected" list. Sticking to that list — resisting the temptation to add multi-broker Kafka, Kubernetes deployment, or a streaming layer during Phase 2 — was harder than expected but essential. Every rejected feature would have added debugging time without meaningfully improving the delivered product.
- **Honest self-reflection.** The debugging story documented in `docs/02-development.md` §4 is more instructive than a polished final product would have been. Learning to write down what went wrong, rather than hiding it, is a skill in itself.
- **Structured decomposition.** Breaking the project into three phases with concrete deliverables, and treating tutor feedback as a genuine input rather than a hurdle, produced better work than a monolithic effort would have. This maps directly to how real engineering projects are structured with milestones and reviews.

## 9. Repository and reproducibility

The full source code, documentation, and screenshots are at:

**GitHub repository:** (https://github.com/Hagunamata/dark-factory-data-platform)

To reproduce the environment on a fresh machine with Docker installed:

```bash
git clone <repository URL>
cd dark-factory-data-platform
cp .env.example .env       # review and adjust values if needed
docker compose up -d       # bring up all eleven services
docker compose exec data-generator python -m data_generator.bootstrap
# (trigger the DAG from the Airflow UI or via make demo)
```

The full verification procedure is in `docs/verification.md`. Screenshots corresponding to each verification step are in `docs/screenshots/`.

## 10. Closing note

The project delivers what the brief asked for: a reproducible, containerized batch data infrastructure that handles a million time-referenced data points, demonstrates canonical data engineering patterns (durable ingestion, decoupled storage layers, orchestrated transformations, centralised observability), and is honest about the boundary between what is implemented and what would be needed at production scale.

The technical work is the visible part of the deliverable. The less visible part — the discipline of documenting design decisions before implementation, of writing down what broke and why, and of treating scope reduction as a virtue rather than a compromise — is what turns a working prototype into a portfolio piece worth showing.

---

## References

Gisi, P. (2024). *The Dark Factory and the Future of Manufacturing: A Guide to Operational Efficiency and Competitiveness*. New York, NY: Routledge, Taylor & Francis Group. p. 3. doi:10.4324/9781032688152

Kafka.apache.org. (2026) Introduction. https://kafka.apache.org/42/getting-started/introduction/

Kleppmann, M. (2017). Designing data-intensive applications: the big ideas behind reliable, scalable, and maintainable systems. *Heidelberg O'Reilly*. 2017 1st Edition. p. 38 ~ 41, 488 ~ 490

Spark.apache.org. Apache Spark - A Unified engine for large-scale data analytics. https://spark.apache.org/docs/latest/
