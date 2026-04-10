FROM apache/spark:3.5.0

USER root

WORKDIR /app
COPY . /app

RUN pip install pyspark kafka-python

USER 1001