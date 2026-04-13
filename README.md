# traffic-pipeline


#### Architecutre Overview


##### Preprocessing Instructions
From the **repository root**, place the INRIX CSV under `data/` (see paths in `preprocessing-scripts/prune.py`) and run, in order:

`python preprocessing-scripts/prune.py` → `python preprocessing-scripts/consolidate_parquet.py` → `python preprocessing-scripts/upload_parquets.py`

For `airflow/scripts/spark_batch.py`, upload `XD_Identification.csv` to `s3://ndot-traffic-pipeline/raw/XD_Identification.csv` (same bucket as Parquet; not handled by `upload_parquets.py`).
Create a `.env` file in the project (e.g. repo root) with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and optional AWS_SESSION_TOKEN / AWS_REGION. load_dotenv() loads it when you run the script so boto3 sees those vars—no credentials in source code.

TODO: 
 - Put more information about setting up the S3 bucket and make sure that the code in there works with easily changed variables
 - Could also create a jupyter notebook or .sh that just runs those 3 files one after the other
 - Put more information about what each `.py` file does


 #### Instructions to run
 To start docker containers, run `docker-compose up --build`. Each service has its own docker container
 IMPORTANT: I kept the spark scripts in `airflow/scripts` but they should probably be seperated into the `spark` folder and a trigger in the `airflow/scripts`
 To see streamlit dashboard, go to `localhost:8501`
 Will need to docker exec into the producer container to be able to start up the producer

 #### Testing Kafka producer locally

Prerequisites:
- Create a `.env` file in the project root with AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
- S3 data must be in the folder structure: `s3://ndot-traffic-pipeline/raw/year=YYYY/month=M/data.parquet`

Steps:
1. Start the Kafka, producer, and consumer services:
    ```bash
    docker-compose up --build kafka producer-app kafka-consumer
    ```
    - The `kafka-consumer` service will automatically start consuming from the `road-segments` topic
    - Kafka has a healthcheck; consumer waits for Kafka to be healthy before connecting

2. In another shell, exec into the producer container and run the producer:
    ```bash
    docker exec -it <producer-app-container-id> bash
    python producer.py --start-time 2023-01-01T00:00:00 --end-time 2023-03-31T23:59:59 --rate 1 --run-duration 60
    ```
    - The producer loads only the necessary months from S3 based on your time range
    - `--rate 1` means one full pass through the data per second
    - `--run-duration 60` limits execution to 60 seconds

3. View consumer logs in another shell:
    ```bash
    docker logs -f <kafka-consumer-container-id>
    ```
    Each message will be logged as JSON with topic, partition, offset, key, and value.

4. To find container IDs:
    ```bash
    docker ps | grep kafka-consumer
    docker ps | grep producer-app
    ```


