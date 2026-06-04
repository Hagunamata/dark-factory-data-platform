# Setup Guide — Dark Factory Data Platform

This guide explains how to bring the stack online on **Windows 10/11** and **Ubuntu 24.04**, including all dependencies, common pitfalls, and verification steps.

The platform is fully containerised — you do **not** need to install Postgres, Kafka, Spark, Airflow, or the ELK stack on your host. Docker provides all of them. The only things you install on the host are: Docker, Python (optional, for tests), Git, and — optionally — `make`.

---

## 1. Resource requirements

| Resource | Minimum | Recommended |
|---|---|---|
| RAM allocated to Docker | 6 GB | 8 GB |
| CPU cores allocated to Docker | 4 | 6 |
| Free disk space | 15 GB | 25 GB |
| Internet bandwidth (first run) | ~8 GB of image downloads | — |

Elasticsearch alone reserves a 1 GB JVM heap; Spark master + worker reserve ~2 GB; Airflow webserver + scheduler ~1 GB; Kafka ~512 MB. The defaults that ship with Docker Desktop on Windows (2 GB) **will OOM**.

---

## 2. Common dependencies (both OSes)

| Tool | Why | Required? |
|---|---|---|
| **Docker Engine + Compose v2** | Runs every service in the stack | **Yes** |
| **Git** | Clone the repository | **Yes** |
| **GNU Make** | Convenience wrapper around `docker compose` | Optional — the Makefile is sugar; every target can be run as a raw `docker compose` command |
| **Python 3.11+** | Only needed if you want to run the data generator outside its container (e.g. for unit tests) | Optional |

> **Important:** the project ships a `Makefile`, but it is **purely a convenience wrapper**. Every `make <target>` maps to a single `docker compose ...` invocation. If `make` doesn't work on your platform, run the underlying command directly — see §5.

---

## 3. Windows 10 / 11

### 3.1 Install Docker Desktop

```powershell
winget install -e --id Docker.DockerDesktop
```

After install:

1. **Reboot** (Docker Desktop enables WSL2 / Hyper-V on first install).
2. Launch **Docker Desktop** from the Start menu. Wait until the whale icon in the system tray says **"Engine running"**.
3. Open **Settings → Resources → Advanced** and set:
   - Memory: **≥ 6 GB** (8 GB recommended)
   - CPUs: **≥ 4**
   - Disk image size: ≥ 40 GB
4. Apply & Restart.

> **Licensing note:** Docker Desktop is free for personal use, education, and small companies (<250 employees AND <$10M revenue). At a larger employer, use **Rancher Desktop** instead:
> ```powershell
> winget install -e --id SUSE.RancherDesktop
> ```
> Same `docker` CLI, no per-seat license.

Verify in a **new** PowerShell window:

```powershell
docker --version
docker compose version
```

Both must print a version. If "docker is not recognized," Docker Desktop isn't running or your PATH didn't refresh — close every PowerShell window and open a fresh one.

### 3.2 Install Git

```powershell
winget install -e --id Git.Git
```

Open a new PowerShell window after install.

### 3.3 (Optional) Install GNU Make

If you want to use the `make` shortcuts:

```powershell
winget install -e --id GnuWin32.Make
```

This installs `make.exe` to `C:\Program Files (x86)\GnuWin32\bin\`. winget does **not** automatically add it to PATH. Add it permanently:

```powershell
[Environment]::SetEnvironmentVariable(
    "Path",
    [Environment]::GetEnvironmentVariable("Path", "User") + ";C:\Program Files (x86)\GnuWin32\bin",
    "User"
)
```

Then close and reopen PowerShell. Verify: `make --version`.

> **Make on Windows caveat:** GnuWin32 make defaults to executing recipes via `CreateProcess` rather than a shell, which breaks commands like `docker compose` (two words). To force it to use `cmd.exe`, add this at the top of the `Makefile`:
> ```makefile
> SHELL := cmd.exe
> .SHELLFLAGS := /c
> ```
> If you hit any other make weirdness on Windows, **bypass it and use `docker compose` directly (§5)**. The Makefile is purely a shortcut.

### 3.4 (Recommended) Use WSL2 instead

The cleanest path on Windows is to run everything from inside WSL2 Ubuntu, and let Docker Desktop's WSL2 backend handle the containers. Then you're effectively following the Ubuntu instructions (§4) and `make` works natively.

```powershell
wsl --install -d Ubuntu-24.04
```

Reboot, set up your Ubuntu username/password, then inside the Ubuntu shell follow §4 — except you **skip §4.1** (Docker Engine install): Docker Desktop on Windows automatically exposes the `docker` CLI inside WSL2 distros if you enable it in **Settings → Resources → WSL Integration**.

### 3.5 Clone and configure

```powershell
cd "C:\Users\HakwoonChung\OneDrive - Neura Robotics GmbH\Private\Study\Project, Data Engineering\Source"
# repo is already here, just verify:
cd dark-factory-data-platform
Copy-Item .env.example .env
```

Edit `.env` if you want non-default ports or credentials (the defaults are fine for local dev).

### 3.6 Bring the stack up

```powershell
docker compose up -d
```

First run pulls ~8 GB of images and builds the custom Airflow image (OpenJDK + pyspark, ~600 MB layer). Expect **5–15 minutes** of download/build on first invocation; subsequent `up` is seconds.

Watch progress:

```powershell
docker compose ps
docker compose logs -f --tail=50
```

When `docker compose ps` shows all services with status `Up (healthy)` or `Up`, proceed.

### 3.7 Seed the data

```powershell
docker compose exec data-generator python -m data_generator.bootstrap
```

Expected output: `Bootstrap complete: 600000 logistics + 400000 hrss = 1000000 rows total`.

### 3.8 Tear down

```powershell
docker compose down            # stop containers, keep data volumes
docker compose down -v         # stop containers AND delete all data (full reset)
```

---

## 4. Ubuntu 24.04

### 4.1 Install Docker Engine + Compose v2

Ubuntu's default `docker.io` package is outdated. Use Docker's official apt repository:

```bash
# Remove any old Docker installs
for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do
    sudo apt-get remove -y $pkg
done

# Add Docker's official GPG key and apt source
sudo apt-get update
sudo apt-get install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

echo "deb [arch=$(dpkg --print-architecture) \
    signed-by=/etc/apt/keyrings/docker.asc] \
    https://download.docker.com/linux/ubuntu \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install
sudo apt-get update
sudo apt-get install -y \
    docker-ce \
    docker-ce-cli \
    containerd.io \
    docker-buildx-plugin \
    docker-compose-plugin
```

### 4.2 Post-install: run docker without sudo

```bash
sudo usermod -aG docker $USER
newgrp docker        # apply group in current shell — or log out + back in
```

Verify:

```bash
docker run --rm hello-world
docker compose version
```

### 4.3 Set Docker resource limits

Docker Engine on Linux uses the host's full RAM and CPU by default — no GUI knob to configure like Docker Desktop. The stack will simply allocate up to its declared limits (no host-level reservation needed). If you're on a low-RAM machine (< 8 GB), reduce Elasticsearch's heap in `docker-compose.yml`:

```yaml
ES_JAVA_OPTS: "-Xms512m -Xmx512m"
```

### 4.4 Install Git and make

```bash
sudo apt-get install -y git build-essential
```

`build-essential` includes `make`. The Makefile in this repo is written for GNU Make under a POSIX shell, so it works out of the box on Ubuntu.

### 4.5 Clone and configure

```bash
git clone <your-fork-url> dark-factory-data-platform
cd dark-factory-data-platform
cp .env.example .env
```

### 4.6 Bring the stack up

```bash
make up
# or, equivalently:
docker compose up -d
```

Verify:

```bash
make status
# or:
docker compose ps
```

### 4.7 Seed the data

```bash
make seed
# or:
docker compose exec data-generator python -m data_generator.bootstrap
```

### 4.8 Tear down

```bash
make down            # stop, keep volumes
make clean           # stop AND delete volumes
make nuke            # full reset including local generated files
```

---

## 5. `make` → `docker compose` cheat sheet

If `make` is unavailable or misbehaving, every target maps 1:1 to a `docker compose` command:

| Makefile target | Equivalent direct command |
|---|---|
| `make up` | `docker compose up -d` |
| `make down` | `docker compose down` |
| `make restart` | `docker compose restart` |
| `make logs` | `docker compose logs -f --tail=100` |
| `make ps` | `docker compose ps` |
| `make status` | `docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"` |
| `make seed` | `docker compose exec data-generator python -m data_generator.bootstrap` |
| `make demo` | `docker compose exec airflow-webserver airflow dags trigger dark_factory_pipeline` |
| `make clean` | `docker compose down -v` |
| `make nuke` | `docker compose down -v` + manually delete `data/`, `generated/`, `airflow/logs/` |

---

## 6. Service URLs after `up`

| Service | URL | Default credentials |
|---|---|---|
| Airflow web UI | http://localhost:8080 | `airflow` / `airflow` |
| Spark master UI | http://localhost:8081 | none |
| Kibana | http://localhost:5601 | none |
| Elasticsearch | http://localhost:9200 | none |
| Postgres | `localhost:5432` | see `.env` |
| Kafka broker | `localhost:9092` | none |

---

## 7. Verification checklist

After `docker compose up -d` and `docker compose exec data-generator python -m data_generator.bootstrap`:

1. **All containers healthy**
   ```bash
   docker compose ps
   ```
   Every row should be `Up` or `Up (healthy)`. If any service is `Restarting`, check its logs.

2. **Kafka has the data**
   ```bash
   docker compose exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
       --broker-list localhost:9092 --topic logistics_events
   docker compose exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
       --broker-list localhost:9092 --topic hrss_telemetry
   ```
   Offsets should sum to ~600 000 and ~400 000 respectively.

3. **Postgres schemas exist**
   ```bash
   docker compose exec postgres psql -U darkfactory -d darkfactory \
       -c "\dn"     # should list 'raw' and 'analytics'
   docker compose exec postgres psql -U darkfactory -d airflow \
       -c "\dt"     # should list Airflow's metadata tables
   ```

4. **Airflow web UI loads** at http://localhost:8080 with no DAG import errors.

5. **Spark master UI** at http://localhost:8081 shows one worker registered.

6. **Kibana** at http://localhost:5601 shows the welcome screen (Elasticsearch index pattern setup is Phase 2 Step 6).

---

## 8. Troubleshooting

### "docker is not recognized" (Windows)
Docker Desktop isn't running or your shell predates the install. Close every PowerShell window, confirm Docker Desktop's tray icon says "Engine running", open a fresh PowerShell.

### `docker compose` exits immediately with no error
You're missing the v2 plugin. Run `docker compose version` — if it errors, you have the legacy `docker-compose` only. On Ubuntu, install `docker-compose-plugin` from the official repo (§4.1). On Windows, reinstall Docker Desktop.

### Elasticsearch exits with `exit code 137`
OOM kill. Either give Docker more RAM (Windows: Docker Desktop → Settings → Resources) or lower `ES_JAVA_OPTS` in `docker-compose.yml` to `-Xms512m -Xmx512m`.

### `airflow-init` fails with `database "airflow" does not exist`
The Postgres init script didn't run. This happens when an old `postgres_data` volume exists from before the `00_create_databases.sql` file was added. Fix:
```bash
docker compose down -v       # WARNING: deletes all data
docker compose up -d
```

### Kafka container restarts forever with "Cluster ID mismatch"
The on-disk KRaft metadata is from an older Kafka image version. Fix:
```bash
docker compose down
docker volume rm dark-factory-data-platform_kafka_data
docker compose up -d
```

### `make: command not found` (Windows)
Either install GNU Make (§3.3) or just use the equivalent `docker compose` commands (§5).

### `process_begin: CreateProcess(NULL, docker compose up -d, ...) failed` (Windows + GnuWin32 make)
GnuWin32 make isn't using a shell. Either add `SHELL := cmd.exe` / `.SHELLFLAGS := /c` at the top of the Makefile, or bypass make entirely (§5).

### Port already in use
Another local service is on 5432 / 8080 / 9092 / 5601 / 9200. Either stop the conflicting service, or change the host-side port in `.env` (e.g. `POSTGRES_PORT=5433`).

### Slow first build
The custom Airflow image installs OpenJDK + pyspark on top of the Airflow base image, so the first `docker compose up -d` takes 5–15 minutes. Subsequent runs reuse the cached layers and start in seconds.

### Spark UI shows 0 workers
Check `docker compose logs spark-worker`. The worker waits for `spark-master` to become healthy; if the master never becomes healthy, Java may be OOMing. Lower `SPARK_WORKER_MEMORY` from `2G` to `1G` in `docker-compose.yml`.

---

## 9. Where things live (mental map)

```
host filesystem                          inside the container
─────────────────                        ────────────────────
./data_generator/      ←—mount—→         /app/data_generator   (data-generator service)
./sample_data/         ←—mount—→         /app/sample_data      (data-generator service)
./airflow/dags/        ←—mount—→         /opt/airflow/dags     (airflow services)
./spark/jobs/          ←—mount—→         /opt/spark/jobs       (airflow + spark services)
./postgres/init/       ←—mount—→         /docker-entrypoint-initdb.d  (postgres, runs once)
./elk/logstash/        ←—mount—→         /usr/share/logstash/pipeline (logstash service)

named docker volumes (persistent across `down`, deleted by `down -v`):
  postgres_data        — Postgres data files
  kafka_data           — Kafka KRaft metadata + logs
  elasticsearch_data   — Elasticsearch indices
  airflow_logs         — Airflow task logs
```

This is what makes the stack reproducible: every byte of state outside `volumes:` lives on your host filesystem and is version-controlled.
