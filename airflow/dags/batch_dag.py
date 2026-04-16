"""Historical aggregations: run Spark batch against data already in S3.

Manual DAG. Assumes preprocessing (or equivalent) has populated the bucket and
that /opt/airflow/scripts/spark_batch.py matches the Spark image / deps you use elsewhere.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="batch_pipeline",
    description="Run Spark batch job to compute historical aggregations from S3",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_spark_batch = BashOperator(
        task_id="run_spark_batch",
        bash_command="python /opt/airflow/scripts/spark_batch.py",
    )
