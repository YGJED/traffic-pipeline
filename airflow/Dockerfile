FROM apache/airflow:2.10.3
USER root
RUN apt-get update && apt-get install -y default-jdk docker-compose-plugin && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install \
    pyspark==4.1.1 \
    python-dotenv \
    boto3 \
    pyarrow \
    pandas