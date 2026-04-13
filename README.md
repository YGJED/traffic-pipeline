# Traffic Pipeline

End-to-end traffic analytics project for Davidson County using INRIX data.

## What This Project Has

| Pipeline | Purpose | Main Script | Output |
|---|---|---|---|
| Preprocessing | Convert raw CSV into partitioned Parquet and upload to S3 | `preprocessing-scripts/*.py` | `s3://ndot-traffic-pipeline/raw/...` |
| Streaming | Replay time slices to Kafka and compute live Spark metrics | `producer/producer.py` + `spark/streaming_job.py` | `s3://ndot-traffic-pipeline/live/...` |
| Batch | Run historical aggregations with Spark | `airflow/scripts/spark_batch.py` | `s3://ndot-traffic-pipeline/historical/...` |

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

- `preprocessing-scripts/` - CSV -> Parquet -> S3 preparation scripts
- `producer/` - Kafka producer + debug consumer
- `spark/` - Spark Structured Streaming app
- `airflow/` - Airflow services + batch Spark script
- `streamlit/` - dashboard app
- `docker-compose.yml` - service orchestration

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

## One-Time Data Preparation (For Both Pipelines)

1. Put CSV at:
   - `data/Davidson-2023-2024-for-NDOT-10-min-Ave/Davidson-2023-2024-for-NDOT-10-min-Ave.csv`

2. Build local parquet partitions:

```powershell
python preprocessing-scripts/prune.py
python preprocessing-scripts/consolidate_parquet.py
```

3. Upload partitioned parquet to S3:

```powershell
python -c "from preprocessing-scripts.upload_parquets import upload_all; upload_all()"
```

4. Upload lookup file used by Spark:
   - local: `data/Davidson-2023-2024-for-NDOT-10-min-Ave/XD_Identification.csv`
   - S3: `s3://ndot-traffic-pipeline/raw/XD_Identification.csv`

---

## Streaming Pipeline Runbook

### Correct Startup Order (Important)

Spark uses `startingOffsets=latest`, so startup order matters.

1. Start Kafka + Zookeeper:

```powershell
docker compose up -d zookeeper kafka
```

2. Ensure Kafka topic exists (recommended before Spark startup).

Even with `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`, Spark can still fail on startup if the topic is not ready yet. Create and verify explicitly:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --create --topic road-segments --partitions 16 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic road-segments
```

Note: review `Tuning And Configuration` -> `Kafka topic partitions` for partition-count guidance.

3. Start Spark services:

```powershell
docker compose up -d spark-master spark-worker spark-app
```

4. Wait for Spark streaming startup:

```powershell
docker compose logs -f spark-app
```

Spark is ready when you see idle messages like:

```text
INFO MicroBatchExecution: Streaming query has been idle and waiting for new data more than 10000 ms.
```

5. Run producer after Spark is ready:

```powershell
docker compose up -d producer-app
docker compose exec producer-app python producer.py --start-time 2023-01-01T00:00:00 --end-time 2023-01-02T00:00:00 --emit-mode verbose --slice-delay 1
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
3. Restart Spark (`spark-app` at minimum)
4. Re-run producer

Usually no need to restart Kafka/Zookeeper unless unhealthy.

#### Re-run same producer range without clearing checkpoint

- Kafka gets new offsets for re-sent messages.
- Spark may not write new output for already-finalized windows (depends on checkpoint/watermark state).

---

## Batch Pipeline Runbook

Batch script: `airflow/scripts/spark_batch.py`

### What it does

- Reads historical raw data from `s3://ndot-traffic-pipeline/raw/year=2023`
- Joins with `s3://ndot-traffic-pipeline/raw/XD_Identification.csv`
- Computes historical aggregates
- Writes to `s3://ndot-traffic-pipeline/historical/*`

### Run Batch Job (Local Python)

```powershell
python airflow/scripts/spark_batch.py
```

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
.config("spark.sql.shuffle.partitions", "4")
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
