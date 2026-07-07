# `airflow/jars/`

JARs that `spark-submit` needs at runtime. Bind-mounted into the Airflow container at `/opt/spark/extra-jars/`.

## Required files

| File | Where to get it |
|---|---|
| `postgresql-42.7.3.jar` | https://jdbc.postgresql.org/download/postgresql-42.7.3.jar |

## One-time download

```bash
# Linux / WSL2
curl -fsSL -o airflow/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
```

```powershell
# Windows PowerShell
Invoke-WebRequest `
    -Uri "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar" `
    -OutFile "airflow\jars\postgresql-42.7.3.jar"
```

Verify inside the container:

```bash
docker compose exec airflow-scheduler ls -la /opt/spark/extra-jars/
```
