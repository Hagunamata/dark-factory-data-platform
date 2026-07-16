# Development Phase — PebblePad text (≈200 words)

**Author:** Hakwoon Chung
**Phase:** 2 — Development / Reflection

The batch data infrastructure designed in Phase 1 was implemented as a containerised microservice stack in a Linux (Ubuntu) environment, orchestrated through a single Docker Compose file covering eleven services. The stack includes Apache Kafka as the ingestion buffer, PostgreSQL holding both a `raw` and an `analytics` schema, Apache Spark performing the quarterly aggregation, Apache Airflow scheduling the pipeline, and an ELK stack collecting container logs via Docker's GELF driver.

A custom Python data generator reads two Kaggle reference datasets (Smart Logistics and HRSS), builds empirical distributions, and produces one million synthetic timestamped rows into Kafka. An Airflow DAG drains both topics into the raw schema, then invokes a PySpark job that aggregates the quarter into ML-ready feature tables. The Spark write is made idempotent by deleting and re-inserting the target quarter's rows.

The end-to-end pipeline was verified running locally, with all eleven services healthy and analytics tables populated as expected. Reproducibility is achieved through Infrastructure as Code — the entire environment is brought up with a single `docker compose up` command. Documentation, verification steps, and screenshots are included in the repository.

**GitHub repository:** [https://github.com/Hagunamata/dark-factory-data-platform]
