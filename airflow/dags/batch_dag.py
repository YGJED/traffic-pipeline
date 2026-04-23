"""Historical aggregations: run Spark batch against data already in S3.

Manual DAG. spark_batch processes all 12 months of 2023 in a single Spark
job, then spark_consolidate merges each monthly parquet output into one
file per aggregation type under year=2023 in S3.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

REPO = "/opt/airflow/repo"
COMPOSE = f"cd {REPO} && docker compose"

with DAG(
    dag_id="batch_pipeline",
    description="Run Spark batch job to compute historical aggregations from S3",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_spark_batch = BashOperator(
        task_id="run_spark_batch",
        bash_command=(
            f"{COMPOSE} down && "
            f"{COMPOSE} up --build --scale spark-worker=3 "
            f"--exit-code-from spark-batch spark-batch && "
            f"{COMPOSE} down"
        ),
    )

    consolidate_year = BashOperator(
        task_id="consolidate_year",
        bash_command=(
            f"{COMPOSE} down && "
            f"{COMPOSE} up --build --scale spark-worker=3 "
            f"--exit-code-from spark-consolidate spark-consolidate && "
            f"{COMPOSE} down"
        ),
    )

    run_spark_batch >> consolidate_year
