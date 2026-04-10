import time
import json
import io
import os
import argparse
from datetime import datetime

import boto3
import pandas as pd
import pyarrow.parquet as pq
from kafka import KafkaProducer
from dotenv import load_dotenv, find_dotenv

# Example usage:
# python producer.py \
#   --start-time 2023-01-01T00:00:00 \
#   --end-time 2023-01-01T03:00:00 \
#   --rate 1 \
#   --run-duration 600

# one line version:
# python producer.py --start-time 2023-01-01T00:00:00 --end-time 2023-01-01T03:00:00 --rate 1 --run-duration 600

# =========================
# ARGUMENT PARSING
# =========================

def parse_args():
    parser = argparse.ArgumentParser(description="Kafka S3 Streaming Producer")

    parser.add_argument(
        "--start-time",
        type=str,
        required=True,
        help="ISO start time (e.g. 2023-01-01T00:00:00)"
    )

    parser.add_argument(
        "--end-time",
        type=str,
        default=None,
        help="ISO end time (optional)"
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=1.0,
        help="Seconds between each full dataset pass"
    )

    parser.add_argument(
        "--run-duration",
        type=int,
        default=None,
        help="Max runtime in seconds (optional)"
    )

    return parser.parse_args()


args = parse_args()

START_TIME = datetime.fromisoformat(args.start_time)
END_TIME = datetime.fromisoformat(args.end_time) if args.end_time else None
RATE_SECONDS = args.rate
RUN_DURATION = args.run_duration

# =========================
# CONFIGURATION
# =========================

S3_BUCKET = "ndot-traffic-pipeline"
S3_PREFIX = "raw/year=2023"

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "road-segments")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# =========================
# AWS S3 CLIENT
# =========================

load_dotenv(find_dotenv())
s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))

# =========================
# KAFKA PRODUCER
# =========================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),

    # batching + throughput
    batch_size=131072,        # 128KB
    linger_ms=20,             # allow batching
    compression_type="lz4",   # fast compression

    # reliability
    acks="all"
)

# =========================
# LOAD DATA FROM S3
# =========================

def list_parquet_files(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    files = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(obj["Key"])

    return files


def load_parquet_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    buffer = io.BytesIO(response["Body"].read())
    table = pq.read_table(buffer)
    return table.to_pandas()


def load_all_data():
    files = list_parquet_files(S3_BUCKET, S3_PREFIX)

    dfs = []
    for key in files:
        print(f"Loading {key}")
        df = load_parquet_from_s3(S3_BUCKET, key)
        dfs.append(df)

    if not dfs:
        raise ValueError("No parquet files found in S3")

    df = pd.concat(dfs, ignore_index=True)

    # Ensure datetime
    df["measurement_tstamp"] = pd.to_datetime(df["measurement_tstamp"])

    # Filter time range
    df = df[df["measurement_tstamp"] >= START_TIME]

    if END_TIME:
        df = df[df["measurement_tstamp"] <= END_TIME]

    # Sort for deterministic streaming
    df = df.sort_values("measurement_tstamp")

    return df

# =========================
# STREAM TO KAFKA
# =========================

def stream_data(df):
    start_wall_time = time.time()

    while True:
        loop_start = time.time()

        for row in df.itertuples(index=False):
            # Run duration check
            if RUN_DURATION is not None:
                if time.time() - start_wall_time > RUN_DURATION:
                    print("Reached run duration limit. Stopping.")
                    producer.flush()
                    return

            message = {
                "xd_id": row.xd_id,
                "measurement_tstamp": row.measurement_tstamp.isoformat(),
                "speed": row.speed,
                "reference_speed": row.reference_speed,
                "confidence_score": row.confidence_score,
            }

            # Keyed by xd_id → ensures partition consistency
            producer.send(
                KAFKA_TOPIC,
                key=str(row.xd_id).encode("utf-8"),
                value=message
            )

        producer.flush()

        # Maintain pacing
        elapsed = time.time() - loop_start
        sleep_time = max(0, RATE_SECONDS - elapsed)
        time.sleep(sleep_time)

        print(f"Completed one full pass in {elapsed:.2f}s")

# =========================
# MAIN
# =========================

if __name__ == "__main__":
    df = load_all_data()
    stream_data(df)