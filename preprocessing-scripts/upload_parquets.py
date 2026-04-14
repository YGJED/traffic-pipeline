import os
import boto3
from dotenv import load_dotenv, find_dotenv

# Create a `.env` file in the project (e.g. repo root) with AWS_ACCESS_KEY_ID,
# AWS_SECRET_ACCESS_KEY, and optional AWS_SESSION_TOKEN / AWS_REGION. load_dotenv() loads
# it when you run the script so boto3 sees those vars—no credentials in source code.
load_dotenv(find_dotenv())
s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))

BUCKET = os.getenv("S3_BUCKET", "ndot-traffic-pipeline")
S3_PREFIX = "raw/year=2023"

print(BUCKET)

def upload_parquet_structure(local_root, s3_prefix):
    for root, dirs, files in os.walk(local_root):
        for file in files:
            if file.endswith(".parquet"):
                local_path = os.path.join(root, file)

                # extract month from path (expects month=*)
                parts = root.split(os.sep)
                month_part = [p for p in parts if p.startswith("month=")]
                
                if not month_part:
                    continue  # skip anything unexpected

                month = month_part[0].split("=")[1]

                s3_key = f"{s3_prefix}/month={month}/data.parquet"
                print(s3_key)

                print(f"Uploading {local_path} -> s3://{BUCKET}/{s3_key}")
                s3.upload_file(local_path, BUCKET, s3_key)


def upload_all():
    # Before, only the historical parquet tree was pushed to S3, so raw/year=2023 had
    # Jan–May but not the stream months. Run the same upload for both local roots.
    upload_parquet_structure("inrix_historical_parquet/year=2023", S3_PREFIX)
    upload_parquet_structure("inrix_stream_parquet/year=2023", S3_PREFIX)

# run it
upload_all()