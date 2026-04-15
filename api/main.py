from fastapi import FastAPI, HTTPException
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
import os
import s3fs
import numpy as np
import math

load_dotenv("/app/.env")

app = FastAPI(title="Traffic Pipeline API", description="API for traffic data")

S3_BUCKET = os.getenv("S3_BUCKET", "ndot-traffic-pipeline")
S3_PREFIX = "live/latest_congestion_by_segment/"  # Adjust to match your S3 path

def snapshot_is_ready(bucket: str, prefix: str) -> bool:
    """Return True only if _SUCCESS exists in the snapshot prefix."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN")
    )
    try:
        s3.head_object(Bucket=bucket, Key=f"{prefix.rstrip('/')}/_SUCCESS")
        return True
    except s3.exceptions.ClientError:
        return False
    
def sanitize(obj):
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize(v) for v in obj]
    elif isinstance(obj, (float, np.floating)):
        if math.isnan(obj) or math.isinf(obj):
            return None
    return obj


@app.get("/")
def root():
    return {"message": "Traffic Pipeline API"}

@app.get("/live/segments")
def get_live_segments():
    if not snapshot_is_ready(S3_BUCKET, S3_PREFIX):
        raise HTTPException(
            status_code=503,
            detail="Snapshot not ready yet"
        )
    
    try:
        fs = s3fs.S3FileSystem(
            key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
            token=os.getenv("AWS_SESSION_TOKEN")
        )
        # Use "bucket/path" format without "s3://"
        print(f"{S3_BUCKET}/{S3_PREFIX}")
        df = pd.read_parquet(f"{S3_BUCKET}/{S3_PREFIX}", filesystem=fs)
        data=  df.to_dict(orient="records")
        return sanitize(data)
        
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))