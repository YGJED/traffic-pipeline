from fastapi import FastAPI, HTTPException
import pandas as pd
import boto3
from dotenv import load_dotenv
import os
import s3fs
import numpy as np
import math
import threading
import time
from typing import Any

load_dotenv("/app/.env")

app = FastAPI(title="Traffic Pipeline API", description="API for traffic data")

LIVE_REFRESH_INTERVAL = int(os.getenv("LIVE_REFRESH_INTERVAL", "2"))

_live_cache: list[dict[str, Any]] | None = None
_live_cache_lock = threading.Lock()

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
        skip_instance_cache=True,
        use_listings_cache=False,
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

def load_historical(folder: str, year: int, month: int | None = None) -> pd.DataFrame:
    """
    Reads yearly Parquet file from S3 and optionally filters by month.
    Path: s3://<bucket>/historical/<folder>/year=<year>/
    """
    prefix = f"{S3_HISTORICAL_PREFIX}/{folder}/year={year}"
    fs = get_s3fs()
    s3_path = f"{S3_BUCKET}/{prefix}"

    try:
        df = pd.read_parquet(s3_path, filesystem=fs)

        # Optional filtering by month
        if month is not None:
            df = df[df["month"] == month]

        return df

    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for year={year} in {folder}",
        )
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
# ---------------------------------------------------------------------------
# Routes — live
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"message": "Traffic Pipeline API"}

def _refresh_live_cache():
    global _live_cache
    while True:
        if snapshot_is_ready(S3_BUCKET, S3_LIVE_PREFIX):
            try:
                print("Attemping to update cache")
                fs = get_s3fs()
                df = pd.read_parquet(f"{S3_BUCKET}/{S3_LIVE_PREFIX}", filesystem=fs)
                records = sanitize(df.to_dict(orient="records"))
                # print("updated records")
                with _live_cache_lock:
                    _live_cache = records
            except Exception as e:
                print(f"Live cache refresh failed: {e}")
        time.sleep(LIVE_REFRESH_INTERVAL)


@app.on_event("startup")
def start_live_refresh():
    t = threading.Thread(target=_refresh_live_cache, daemon=True)
    t.start()


@app.get("/live/segments")
def get_live_segments():
    with _live_cache_lock:
        data = _live_cache

    if data is not None:
        return data

    raise HTTPException(status_code=503, detail="Snapshot not ready yet")

# ---------------------------------------------------------------------------
# Routes — historical aggregations
# ---------------------------------------------------------------------------

@app.get("/historical/{folder}")
def get_historical(folder: str, year: int, month: int | None = None):
    if folder not in VALID_FOLDERS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown folder '{folder}'. Valid options: {sorted(VALID_FOLDERS)}",
        )

    if month is not None and (month < 1 or month > 12):
        raise HTTPException(status_code=400, detail="month must be 1-12")

    df = load_historical(folder, year, month)
    return sanitize(df.to_dict(orient="records"))