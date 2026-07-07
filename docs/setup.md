# Setup Guide

Bringing the stack up on a fresh machine.

Everything runs in Docker containers, so you don't install Postgres, Kafka, Spark, Airflow, or the ELK stack on the host. You install Docker (with Compose v2), Git, and — optionally — GNU Make. On top of that, the Postgres JDBC driver JAR needs to be downloaded once by hand; see step 5 below.

## 1. Resource requirements

- 8 GB RAM allocated to Docker (Elasticsearch reserves 1 GB heap alone; the default 2 GB will OOM)
- 4 CPU cores
- 20 GB free disk space
- Internet for the initial image pull (~8 GB)

## 2. Install Docker

**Ubuntu 24.04:** use Docker's official apt repository (not `docker.io`, which is outdated).

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

Verify: `docker run --rm hello-world`.

**Windows 10/11:** install Docker Desktop (or Rancher Desktop as a lightweight alternative).

```powershell
winget install -e --id Docker.DockerDesktop
```

Reboot, launch Docker Desktop, open Settings → Resources and raise Memory to at least 6 GB. Verify in a fresh PowerShell: `docker --version` and `docker compose version` must both print a version.

## 3. Clone and configure

```bash
git clone <this-repo> dark-factory-data-platform
cd dark-factory-data-platform
cp .env.example .env
```

The `.env` defaults are fine for local development.

## 4. Bring the stack up

```bash
docker compose up -d
docker compose ps
```

First run pulls ~8 GB of images and builds the custom Airflow image; expect 5–15 minutes. Every service should eventually report `Up` or `Up (healthy)`; `airflow-init` reports `Exited (0)` once it has finished the one-shot `db migrate` step.

## 5. Add the Postgres JDBC driver

The Spark quarterly aggregation job reads Postgres via JDBC. The driver JAR is bind-mounted from the host into the Airflow container; download it once:

```bash
mkdir -p airflow/jars
curl -fsSL -o airflow/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
```

Windows PowerShell equivalent:

```powershell
New-Item -ItemType Directory -Path airflow\jars -Force
Invoke-WebRequest -Uri "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar" `
    -OutFile "airflow\jars\postgresql-42.7.3.jar"
```

Confirm inside the container:

```bash
docker compose exec airflow-scheduler ls -la /opt/spark/extra-jars/
```

`postgresql-42.7.3.jar` must be listed.

## 6. Seed the data and trigger the DAG

```bash
docker compose exec data-generator python -m data_generator.bootstrap
```

Then open Airflow at `http://localhost:8080` (login `airflow` / `airflow`), unpause `dark_factory_pipeline`, and trigger it. All three tasks should finish green within a few minutes.

Full verification steps and expected outputs are in `docs/verification.md`.

## Service URLs

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| Spark master UI | http://localhost:8081 | — |
| Kibana | http://localhost:5601 | — |
| Elasticsearch | http://localhost:9200 | — |
| Postgres | `localhost:5432` | see `.env` |

## Common problems

**Elasticsearch exits with code 137.** Out-of-memory. Give Docker more RAM (Settings → Resources on Windows, host RAM on Linux), or lower `ES_JAVA_OPTS` in `docker-compose.yml` from `-Xms1g -Xmx1g` to `-Xms512m -Xmx512m`.

**Kafka container restarts with "Cluster ID mismatch".** On-disk KRaft metadata from a previous run doesn't match the new container. Wipe the volume: `docker compose down && docker volume rm dark-factory-data-platform_kafka_data && docker compose up -d`.

**Spark job fails with `ClassNotFoundException: org.postgresql.Driver`.** The JDBC JAR isn't visible inside the Airflow container. Confirm the file exists on the host at `airflow/jars/postgresql-42.7.3.jar` and that the containers were recreated after adding the mount: `docker compose up -d --force-recreate airflow-webserver airflow-scheduler airflow-init`.

**`make` isn't available on Windows.** The `Makefile` is a thin convenience wrapper. If `make` isn't installed, run the underlying `docker compose` commands directly — every Makefile target is one line of `docker compose ...`.
