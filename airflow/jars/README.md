# `airflow/jars/`

Host-side directory bind-mounted into the Airflow containers at `/opt/spark/extra-jars/`. Put any JARs that `spark-submit` needs here.

## Why this exists instead of `RUN curl` in the Dockerfile

The original Phase 2 design used `--packages org.postgresql:postgresql:42.7.3` so spark-submit would resolve the JDBC driver via Maven at submit time. That broke in a corporate-proxy environment where Maven Central was unreachable from inside the container. A second attempt baked the JAR into the image via `RUN curl https://jdbc.postgresql.org/...` — same proxy could block that too, or build-cache might silently skip the layer.

**Bind-mounting wins**: the host does the download once (with whatever VPN / proxy / browser the developer needs), the file is committed alongside the code, and the runtime container needs no network access for it.

## Required files

| File | Purpose | Where to get it |
|---|---|---|
| `postgresql-42.7.3.jar` | JDBC driver for the Spark quarterly aggregation job | https://jdbc.postgresql.org/download/postgresql-42.7.3.jar |

## One-time setup

### Ubuntu / WSL2

```bash
curl -fsSL -o airflow/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
```

### Windows PowerShell

```powershell
Invoke-WebRequest `
    -Uri "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar" `
    -OutFile "airflow\jars\postgresql-42.7.3.jar"
```

### If `jdbc.postgresql.org` is ALSO blocked on your host

Download `postgresql-42.7.3.jar` via your browser from any of:
- https://jdbc.postgresql.org/download/
- https://search.maven.org/artifact/org.postgresql/postgresql/42.7.3/jar
- https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

Place the downloaded file into `airflow/jars/` and confirm the filename is exactly `postgresql-42.7.3.jar`.

## Verify after download

```bash
ls -la airflow/jars/postgresql-42.7.3.jar
# expected: ~1.0 MB

docker compose exec airflow-scheduler ls -la /opt/spark/extra-jars/
# expected: postgresql-42.7.3.jar visible inside the container
```

The mount is read-only inside the container (`:ro` in compose) so no process can accidentally modify the host file.

## Committing the JAR to git?

Up to you. The file is ~1 MB. Committing it makes the repo self-contained at the cost of repo size. Most teams .gitignore JARs and document the download step (as we do here). The choice is documented in `docs/02-development.md` §4.4.
