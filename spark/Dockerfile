FROM apache/spark:3.5.0

USER root

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir kafka-python python-dotenv

USER spark
