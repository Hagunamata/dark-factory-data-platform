# Setup Guide

Everything runs in Docker, so the only host requirements are Docker (with the Compose plugin) and Git. Developed and tested on Ubuntu 24.04.

Give Docker enough memory — the stack runs Elasticsearch, Spark, Airflow, Kafka and Postgres together, so budget around 8 GB of RAM. The first `docker compose up` also pulls several GB of images.

## 1. Install Docker

Use Docker's official apt repository (Ubuntu's own `docker.io` package lags behind):

```bash
sudo apt-get update && sudo apt-get install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
    https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io \
    docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker
```

Check it works: `docker run --rm hello-world`.

## 2. Clone and configure

```bash
git clone <this-repo> dark-factory-data-platform
cd dark-factory-data-platform
cp .env.example .env
```

The `.env` defaults are fine for local use.

## 3. Add the Postgres JDBC driver

The Spark job reads Postgres over JDBC, and the driver JAR is mounted into the Airflow container from `airflow/jars/`. Download it once:

```bash
mkdir -p airflow/jars
curl -fsSL -o airflow/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
```

## 4. Start the stack

```bash
docker compose up -d
docker compose ps
```

First run takes a while (image pulls + the custom Airflow image build). When it settles, every service shows `Up` / `Up (healthy)`, except `airflow-init`, which runs once and exits with `Exited (0)`.

## 5. Seed the data and run the pipeline

```bash
docker compose exec data-generator python -m data_generator.bootstrap
```

Then open Airflow at http://localhost:8080 (login `airflow` / `airflow`), unpause `dark_factory_pipeline`, and trigger it. The full verification steps are in `docs/verification.md`.

## Service URLs

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| Spark master UI | http://localhost:8081 | — |
| Kibana | http://localhost:5601 | — |
| Elasticsearch | http://localhost:9200 | — |
| Postgres | `localhost:5432` | see `.env` |
