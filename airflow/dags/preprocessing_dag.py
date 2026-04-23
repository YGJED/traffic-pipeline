"""One-off data prep: INRIX CSV → cleaned Parquet → S3.

Triggered manually (schedule=None). Runs prune → consolidate → upload in order.
Requires raw inputs and env (e.g. AWS) available inside the Airflow container paths.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="preprocessing_pipeline",
    description="Convert raw INRIX CSV to Parquet and upload to S3",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # trigger from UI or API; not on a cron
    catchup=False,  # no backfill of missed intervals when schedule is set later
) as dag:
    # Drop unwanted columns / rows per scripts/prune.py rules
    prune = BashOperator(
        task_id="prune",
        bash_command="python -u /opt/airflow/scripts/prune.py",
    )

    # Merge or normalize Parquet layout before upload
    consolidate = BashOperator(
        task_id="consolidate",
        bash_command="python -u /opt/airflow/scripts/consolidate_parquet.py",
    )

    # Push consolidated Parquet to the bucket paths the streaming/batch jobs expect
    upload = BashOperator(
        task_id="upload_to_s3",
        bash_command="python -u /opt/airflow/scripts/upload_parquets.py",
    )

    prune >> consolidate >> upload
