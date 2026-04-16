"""Bring up the Docker streaming stack and replay a fixed time window into Kafka.

Assumes Airflow can run `docker compose` from the same project directory as your
compose file (often requires mounting the repo and setting cwd, or using a wrapper).

Order: Kafka → topic → Spark streaming app → producer (so consumers do not start on a missing topic).

Important: all docker compose commands in this DAG run *inside* the Airflow scheduler
container (via /var/run/docker.sock), not in your host shell. Keep this in mind when
comparing behavior with manual terminal commands.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="streaming_pipeline",
    description="Start Kafka, Spark streaming, and producer for live traffic pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # --- Task 1: Kafka broker ---
    # Starts Zookeeper + Kafka in the background (service names must match docker-compose).
    # We `cd` into /opt/airflow/repo so docker compose can see docker-compose.yml and
    # resolve all relative paths in that file (build contexts and bind mounts).
    # The until-loop blocks until `kafka-topics --list` succeeds inside the kafka container,
    # so later tasks do not run against a broker that is still booting.
    start_kafka = BashOperator(
        task_id="start_kafka",
        bash_command="""
            cd /opt/airflow/repo &&
            docker compose -p traffic-pipeline up -d zookeeper kafka &&
            MAX_TRIES=40 &&
            TRIES=0 &&
            until docker compose -p traffic-pipeline exec kafka kafka-topics \
                --bootstrap-server localhost:9092 --list; do
                TRIES=$((TRIES + 1))
                if [ "$TRIES" -ge "$MAX_TRIES" ]; then
                    echo "Timeout waiting for Kafka to be ready"
                    exit 1
                fi
                echo "Waiting for Kafka..."
                sleep 3
            done
            echo "Kafka is ready"
        """,
    )

    # --- Task 2: Topic for Spark to subscribe ---
    # Name must match what the streaming job uses (e.g. road-segments).
    # --if-not-exists makes the task safe to re-run; partitions should be >= any expected
    # consumer parallelism; replication-factor 1 is normal for local/single-broker setups.
    create_topic = BashOperator(
        task_id="create_kafka_topic",
        bash_command="""
            cd /opt/airflow/repo &&
            docker compose -p traffic-pipeline exec kafka kafka-topics \
                --bootstrap-server kafka:9092 \
                --create --if-not-exists \
                --topic road-segments \
                --partitions 16 \
                --replication-factor 1
        """,
    )

    # --- Task 3: Spark cluster + streaming job ---
    # Brings up master, worker, and the app container that runs the Structured Streaming job.
    # `cd` is required here because spark-app is built from local source (`build: ./spark`).
    # Readiness: wait until Spark reports an executor is RUNNING (cluster ready to process immediately).
    # Build+recreate spark-app so latest streaming code is always used on each DAG run.
    # spark-master/worker are official images, so we start them without --build.
    start_spark = BashOperator(
        task_id="start_spark",
        bash_command="""
            cd /opt/airflow/repo &&
            docker compose -p traffic-pipeline up -d spark-master spark-worker &&
            docker compose -p traffic-pipeline up -d --build --force-recreate spark-app &&
            until docker compose -p traffic-pipeline logs spark-app 2>&1 | grep -q "Executor updated: .* is now RUNNING"; do
                echo "Waiting for Spark executor..."
                sleep 2
            done
            echo "Spark is ready to consume data"
        """,
    )

    # --- Task 4: Historical replay into Kafka ---
    # Starts the producer container and runs producer.py inside it: reads Parquet from S3
    # (per producer config/env) and publishes JSON keyed by xd_id to the topic above.
    # Times are wall-clock bounds on measurement_tstamp; --slice-delay paces replay;
    # tune start/end/slice-delay for the window you want without editing Spark.
    # --build on producer-app picks up producer source changes when this task runs.
    # No ./producer bind mount here: Airflow-launched compose + docker.sock can resolve
    # bind sources differently than host shell runs; image build is more deterministic.
    run_producer = BashOperator(
        task_id="run_producer",
        bash_command="""
            cd /opt/airflow/repo &&
            docker compose -p traffic-pipeline up -d --build producer-app &&
            docker compose -p traffic-pipeline exec producer-app python producer.py \
                --start-time 2023-01-01T00:00:00 \
                --end-time 2023-01-01T05:00:00 \
                --emit-mode verbose \
                --slice-delay 1
        """,
    )

    start_kafka >> create_topic >> start_spark >> run_producer
