# Traffic Pipeline

End-to-end traffic analytics project for Davidson County using INRIX data.

## What This Project Has

| Pipeline | Purpose | Main Script | Output |
|---|---|---|---|
| Preprocessing | Convert raw CSV into partitioned Parquet and upload to S3 | `preprocessing-scripts/*.py` (local), mirrored in `airflow/scripts/*.py` for DAG tasks | `s3://ndot-traffic-pipeline/raw/...` |
| Streaming | Replay time slices to Kafka and compute live Spark metrics | `producer/producer.py` + `spark/streaming_job.py` | `s3://ndot-traffic-pipeline/live/...` |
| Batch | Run historical aggregations with Spark | `spark/spark_batch.py` (local), mirrored in `airflow/scripts/spark_batch.py` for DAG tasks | `s3://ndot-traffic-pipeline/historical/...` |

---

## Architecture At A Glance

```text
Local INRIX CSV
  -> prune.py + consolidate_parquet.py
  -> local parquet partitions (year/month)
  -> upload to S3 raw/

Streaming branch:
S3 raw/ -> producer.py -> Kafka topic road-segments -> streaming_job.py
-> S3 live/{congestion_by_segment, network_congestion_pct, congestion_by_road_type}
-> S3 live/checkpoints/*

Batch branch:
S3 raw/ + XD_Identification.csv -> spark_batch.py
-> S3 historical/*
```

---

## Repository Layout

- `producer/` - Kafka producer + debug consumer image
- `spark/` - Spark Structured Streaming app image
- `airflow/dags/` - orchestration DAGs (`preprocessing_pipeline`, `batch_pipeline`, `streaming_pipeline`)
- `airflow/scripts/` - Airflow task copies/wrappers of preprocessing + batch scripts
- `airflow/` - Airflow image/Dockerfile
- `streamlit/` - dashboard app
- `docker-compose.yml` - service orchestration for both local and Airflow-driven runs

### Local vs Airflow execution model (important)

- **Local/manual runs**: you execute `docker compose ...` from your terminal at repo root.
- **Airflow runs**: the DAG executes `docker compose ...` from inside `/opt/airflow/repo` (mounted repo) via `/var/run/docker.sock`.
- Because of that, this repo intentionally separates some behavior:
  - `producer-app` and `spark-app` rely on image builds (`--build`) instead of source bind mounts, so DAG runs are deterministic.
  - `kafka-consumer` keeps a source bind mount for local debugging convenience only.

---

## Prerequisites

### Required

- Docker Desktop + Docker Compose
- AWS credentials with access to `ndot-traffic-pipeline` bucket
- `.env` in repo root:
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_SESSION_TOKEN` (if temporary creds)
  - `AWS_REGION`

### Local-only extras (if running Spark outside Docker)

- Python 3.10+
- Java 17 (OpenJDK)
- Windows local Spark extras:
  - `winutils.exe` + `hadoop.dll` in `C:\hadoop\bin`
  - `HADOOP_HOME=C:\hadoop`

If you run Spark in Docker (`spark-app`), you usually do not need local Spark setup.

---

## Dependencies

### Python (`requirements.txt`)

- `pyspark>=3.5.1`
- `kafka-python>=2.0.2`
- `boto3>=1.24.28`
- `pyarrow>=15.0.0`
- `python-dotenv>=1.0.0`
- `fastapi>=0.110.0`
- `uvicorn>=0.29.0`
- `streamlit>=1.33.0`
- `pandas>=2.2.1`
- `s3fs>=2023.3.0`
- `folium>=0.16.0`
- `streamlit-folium>=0.20.0`

### Docker images (`docker-compose.yml`)

- `confluentinc/cp-zookeeper:7.6.0`
- `confluentinc/cp-kafka:7.6.0`
- `apache/spark:3.5.0` (master/worker)
- custom Spark app image from `spark/Dockerfile`
- `apache/airflow:2.10.3`
- `postgres:13`
- `python:3.10-slim` (producer + streamlit base)

---

## Local Testing

### Data Ingestion

1. Put CSV at:
   - `data/Davidson-2023-2024-for-NDOT-10-min-Ave/Davidson-2023-2024-for-NDOT-10-min-Ave.csv`

2. Build local parquet partitions (local script paths):

```powershell
python preprocessing-scripts/prune.py
python preprocessing-scripts/consolidate_parquet.py
```

3. Upload partitioned parquet to S3 (local script path):

```powershell
python preprocessing-scripts/upload_parquets.py
```

4. Upload lookup file used by Spark:
   - local: `data/Davidson-2023-2024-for-NDOT-10-min-Ave/XD_Identification.csv`
   - S3: `s3://ndot-traffic-pipeline/raw/XD_Identification.csv`

---

### Streaming

### Correct Startup Order (Important)

Spark uses `startingOffsets=latest`, so startup order matters.

1. Start Kafka + Zookeeper:

```powershell
docker compose -p traffic-pipeline up -d zookeeper kafka
```

2. Ensure Kafka topic exists (recommended before Spark startup).

Even with `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`, Spark can still fail on startup if the topic is not ready yet. Create and verify explicitly:

```powershell
docker compose -p traffic-pipeline exec kafka kafka-topics --bootstrap-server kafka:9092 --create --topic road-segments --partitions 16 --replication-factor 1
docker compose -p traffic-pipeline exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic road-segments
```

Note: review `Tuning And Configuration` -> `Kafka topic partitions` for partition-count guidance.

3. Start Spark services:

```powershell
docker compose -p traffic-pipeline up -d spark-master spark-worker
docker compose -p traffic-pipeline up -d --build --force-recreate spark-app
```

4. Wait for Spark streaming startup:

```powershell
docker compose -p traffic-pipeline logs -f spark-app
```

Spark is ready when you see startup lines like:

```text
INFO MicroBatchExecution: Streaming query has been idle and waiting for new data more than 10000 ms.
```
```text
INFO StandaloneAppClient$ClientEndpoint: Executor updated: ... is now RUNNING
```

You may also see (depending on log mode/version):

```text
Batch: 0
```

5. Run producer after Spark is ready:

```powershell
docker compose -p traffic-pipeline up -d --build producer-app
docker compose -p traffic-pipeline exec producer-app python producer.py --start-time 2023-01-01T00:00:00 --end-time 2023-01-02T00:00:00 --emit-mode verbose --slice-delay 1
```

Note: reference `Tuning And Configuration` -> `Producer runtime flags` for more details on producer options.

6. Verify output in AWS S3 under `s3://ndot-traffic-pipeline/live/`:
   - `congestion_by_segment/`
   - `network_congestion_pct/`
   - `congestion_by_road_type/`
   - parquet part files should appear in each folder

### Why Producer Must Start After Spark

- `startingOffsets=latest` means Spark starts reading at the topic end at query start time.
- If producer ran before Spark startup, those messages can be missed on a fresh run.
- After checkpoint exists, Spark resumes from checkpointed offsets/state.

### Retesting Streaming

#### Clean retest (recommended after Spark logic changes)

1. Stop `spark-app`
2. Clear `s3://ndot-traffic-pipeline/live/`:
   - `checkpoints/`
   - `congestion_by_segment/`
   - `network_congestion_pct/`
   - `congestion_by_road_type/`
   - If this deletion removes the `live/` folder itself, recreate `live/` right away before restarting the pipeline.
3. Restart Spark (`spark-app` at minimum)
4. Re-run producer

Usually no need to restart Kafka/Zookeeper unless unhealthy.

#### Re-run same producer range without clearing checkpoint

- Kafka gets new offsets for re-sent messages.
- Spark may not write new output for already-finalized windows (depends on checkpoint/watermark state).

---

### Batch

Batch script:
- local: `spark/spark_batch.py`
- Airflow DAG task copy: `airflow/scripts/spark_batch.py`

### What it does

- Reads historical raw data from `s3://ndot-traffic-pipeline/raw/year=2023`
- Joins with `s3://ndot-traffic-pipeline/raw/XD_Identification.csv`
- Computes historical aggregates
- Writes to `s3://ndot-traffic-pipeline/historical/*`

### Run Batch Job (Local Python)

```powershell
python spark/spark_batch.py
```

---

## Airflow Usage

Use these commands from repo root.

1. Start Airflow (postgres, init, scheduler, and webserver all start automatically):

```powershell
docker compose up -d airflow-webserver
```

2. Open Airflow UI:
   - URL: [http://localhost:8080](http://localhost:8080)
   - Login: `admin` / `admin`

5. You should see three DAGs:
   - `preprocessing_pipeline`
   - `batch_pipeline`
   - `streaming_pipeline`

Notes:

- `streaming_pipeline` runs docker compose commands from inside Airflow (`/opt/airflow/repo`).
- For streaming tasks, DAG commands rebuild app images (`spark-app`, `producer-app`) so code changes are picked up.

---

## Tuning And Configuration

### Kafka topic partitions

Topic partition count is configured on Kafka side (not inside Spark code).

Create topic with explicit partitions:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --create --topic road-segments --partitions 16 --replication-factor 1
```

Describe topic:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic road-segments
```

Increase partitions for existing topic:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --alter --topic road-segments --partitions 16
```

### Spark shuffle partitions

Configured in `spark/streaming_job.py`:

```python
.config("spark.sql.shuffle.partitions", "8")
```

This controls Spark shuffle parallelism for joins/groupBy/window aggregations.

Starting point for 16 cores:

- Kafka partitions: `8-16`
- Spark shuffle partitions: `16-32` (common first value: `24`)

### Producer runtime flags

`producer/producer.py` options:

- `--start-time` (required): replay start (ISO)
- `--end-time` (optional): replay end, default end of start year
- `--emit-mode {quiet|verbose}`
- `--slice-delay` (seconds)
- `--run-duration` (seconds)
- `--bucket-minutes` (default `10`)

Example:

```powershell
docker compose exec producer-app python producer.py --start-time 2023-01-01T00:00:00 --end-time 2023-01-01T06:00:00 --emit-mode verbose --slice-delay 1 --bucket-minutes 10
```

---

## Useful Commands

Start full stack:

```powershell
docker compose up --build
```

Stop all running services:

```powershell
docker compose down
```

Stop and remove volumes too (full local reset):

```powershell
docker compose down -v
```

Start streaming-only stack:

```powershell
docker compose up -d zookeeper kafka spark-master spark-worker spark-app producer-app
```

Kafka debug consumer:

```powershell
docker compose up -d kafka-consumer
docker compose logs -f kafka-consumer
```

### Service Endpoints (from `docker-compose.yml`)

- Streamlit UI: [http://localhost:8501](http://localhost:8501)
- Airflow UI (when webserver is running): [http://localhost:8080](http://localhost:8080)
- Kafka broker (client connection): `localhost:9092`
- Spark master web UI: [http://localhost:8081](http://localhost:8081)
- Spark master RPC endpoint (for Spark submit): `spark://localhost:7077`

---
