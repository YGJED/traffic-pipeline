from fastapi import FastAPI, HTTPException
import pandas as pd
import boto3
from dotenv import load_dotenv
import os
import s3fs
import numpy as np
import math

load_dotenv("/app/.env")

app = FastAPI(title="Traffic Pipeline API", description="API for traffic data")

S3_BUCKET = os.getenv("S3_BUCKET", "ndot-traffic-pipeline")
S3_LIVE_PREFIX = "live/latest_congestion_by_segment/"
S3_HISTORICAL_PREFIX = "historical"

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def get_s3fs():
    return s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
        token=os.getenv("AWS_SESSION_TOKEN"),
    )

def get_boto3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    )

def sanitize(obj):
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize(v) for v in obj]
    elif isinstance(obj, (float, np.floating)):
        if math.isnan(obj) or math.isinf(obj):
            return None
    return obj

def snapshot_is_ready(bucket: str, prefix: str) -> bool:
    """Return True only if _SUCCESS exists in the snapshot prefix."""
    s3 = get_boto3_client()
    try:
        s3.head_object(Bucket=bucket, Key=f"{prefix.rstrip('/')}/_SUCCESS")
        return True
    except s3.exceptions.ClientError:
        return False

# ---------------------------------------------------------------------------
# Historical data loader
# ---------------------------------------------------------------------------

VALID_FOLDERS = {"by_hour", "by_day_of_week", "by_road_type", "by_direction", "top_segments"}

def load_historical(folder: str, year: int, month: int) -> pd.DataFrame:
    """
    Reads a date-partitioned Parquet folder from S3.
    Path pattern: s3://<bucket>/historical/<folder>/year=<year>/month=<month>/
    """
    prefix = f"{S3_HISTORICAL_PREFIX}/{folder}/year={year}/month={month}"
    fs = get_s3fs()
    s3_path = f"{S3_BUCKET}/{prefix}"

    try:
        # List parquet files under the partition prefix
        files = fs.glob(f"{s3_path}/*.parquet")
        if not files:
            raise FileNotFoundError(f"No parquet files found at s3://{s3_path}")
        return pd.read_parquet(f"s3://{files[0]}", filesystem=fs)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------------------------------------------------------------
# Routes — live
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"message": "Traffic Pipeline API"}

@app.get("/live/segments")
def get_live_segments():
    if not snapshot_is_ready(S3_BUCKET, S3_LIVE_PREFIX):
        raise HTTPException(status_code=503, detail="Snapshot not ready yet")

    try:
        fs = get_s3fs()
        df = pd.read_parquet(f"{S3_BUCKET}/{S3_LIVE_PREFIX}", filesystem=fs)
        return sanitize(df.to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------------------------------------------------------------
# Routes — historical aggregations
# ---------------------------------------------------------------------------

@app.get("/historical/{folder}")
def get_historical(folder: str, year: int, month: int):
    """
    Fetch a historical aggregation partition from S3.

    Query params:
      - year  (int) e.g. 2023
      - month (int) 1-12

    folder must be one of:
      by_hour | by_day_of_week | by_road_type | by_direction | top_segments
    """
    if folder not in VALID_FOLDERS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown folder '{folder}'. Valid options: {sorted(VALID_FOLDERS)}",
        )
    df = load_historical(folder, year, month)
    return sanitize(df.to_dict(orient="records"))
