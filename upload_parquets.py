import os, boto3, json


# NOTE: Do NOT upload AWS information to Github.

credentials = {
    'region_name': 'us-east-1',
    'aws_access_key_id': '***',
    'aws_secret_access_key': '****',
    'aws_session_token': '****'
}

session = boto3.session.Session(**credentials)
s3 = session.client('s3')

BUCKET = "ndot-traffic-pipeline"
LOCAL_ROOT = "inrix_historical_parquet/year=2023"  # adjust if needed
S3_PREFIX = "raw/year=2023"

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

                print(f"Uploading {local_path} -> s3://{BUCKET}/{s3_key}")
                s3.upload_file(local_path, BUCKET, s3_key)


# run it
upload_parquet_structure(LOCAL_ROOT, S3_PREFIX)