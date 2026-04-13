import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, avg, count, when, window,
    approx_count_distinct, current_timestamp, lit,
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, TimestampType
)
from dotenv import load_dotenv

# =========================
# LOAD ENVIRONMENT
# =========================

load_dotenv("/app/.env")

AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN     = os.getenv("AWS_SESSION_TOKEN")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "road-segments")

S3_BUCKET = "ndot-traffic-pipeline"
S3_LIVE   = f"s3a://{S3_BUCKET}/live"

S3_BY_SEGMENT   = f"{S3_LIVE}/congestion_by_segment"
S3_NETWORK_PCT  = f"{S3_LIVE}/network_congestion_pct"
S3_BY_ROAD_TYPE = f"{S3_LIVE}/congestion_by_road_type"

CHECKPOINT_BY_SEGMENT   = f"{S3_LIVE}/checkpoints/by_segment"
CHECKPOINT_NETWORK_PCT  = f"{S3_LIVE}/checkpoints/network_pct"
CHECKPOINT_BY_ROAD_TYPE = f"{S3_LIVE}/checkpoints/by_road_type"

# =========================
# SPARK SESSION
# =========================

spark = (
    SparkSession.builder
    .appName("traffic-streaming")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.session.token", AWS_SESSION_TOKEN)
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# WARN = quiet driver logs. INFO = Spark prints "Streaming query made progress" and
# batch stats each micro-batch (no sink change; works with Parquet → S3).
# Override: set SPARK_LOG_LEVEL=INFO in the environment (e.g. docker-compose).
spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))


# =========================
# KAFKA MESSAGE SCHEMA
# =========================
# Must match exactly what producer.py sends:
# xd_id, measurement_tstamp, speed, reference_speed, confidence_score

message_schema = StructType([
    StructField("xd_id",              LongType(),   True),
    StructField("measurement_tstamp", StringType(), True),
    StructField("speed",              DoubleType(), True),
    StructField("reference_speed",    DoubleType(), True),
    StructField("confidence_score",   DoubleType(), True),
])

# =========================
# READ FROM KAFKA
# =========================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe",               KAFKA_TOPIC)
    .option("startingOffsets",         "latest")
    .option("failOnDataLoss",          "false")
    .load()
)

# =========================
# PARSE JSON + CAST TIMESTAMP
# =========================

parsed = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), message_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("measurement_tstamp").cast(TimestampType()))
)


# =========================
# CONFIGURATION
# =========================

WINDOW_DURATION      = "60 minutes"
SLIDE_DURATION       = "10 minutes"
TRIGGER_INTERVAL     = "10 seconds"
CONGESTION_THRESHOLD = 0.3

# =========================
# FILTER + CLEAN + CONGESTION SCORE
# =========================

cleaned = (
    parsed
    # drop low confidence readings
    .filter(col("confidence_score") >= 20)
    # drop nulls on key fields
    .filter(col("speed").isNotNull())
    .filter(col("reference_speed").isNotNull())
    # avoid division by zero
    .filter(col("reference_speed") > 0)
    # compute congestion score
    .withColumn("congestion_score",
                (col("reference_speed") - col("speed")) / col("reference_speed"))
    # watermark tells Spark how late data can arrive
    .withWatermark("event_time", "10 minutes")
)


# =========================
# LOAD XD LOOKUP (STATIC)
# =========================
# XD_Identification maps xd → road name, frc, geometry (CSV uses start_latitude etc.)
# Loaded once as a static dataframe at startup — not a stream

XD_S3_PATH = f"s3a://{S3_BUCKET}/raw/XD_Identification.csv"

xd_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(XD_S3_PATH)
    .select(
        col("xd"),
        col("road-name").alias("road_name"),
        col("frc"),
        col("bearing"),
        col("start_latitude").alias("start_lat"),
        col("start_longitude").alias("start_long"),
        col("end_latitude").alias("end_lat"),
        col("end_longitude").alias("end_long"),
    )
)

# =========================
# JOIN STREAM WITH XD LOOKUP
# =========================
# Left join so stream rows are kept even if segment ID isn't found in XD
# Drop duplicate "xd" column after join since we already have xd_id from the stream

enriched = (
    cleaned
    .join(xd_df, cleaned.xd_id == xd_df.xd, "left")
    .drop("xd")
)


# =========================
# AGGREGATION 1 — Congestion per segment
# =========================
# Per 60-minute sliding window (10-minute slide): xd_id plus static XD lookup fields
# (road_name, frc, endpoints, bearing). written_at is wall clock when emitted.

agg_by_segment = (
    enriched
    .groupBy(
        window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
        col("xd_id"),
        col("road_name"),
        col("frc"),
        col("start_lat"),
        col("start_long"),
        col("end_lat"),
        col("end_long"),
        col("bearing"),
    )
    .agg(
        avg("speed").alias("avg_speed"),
        avg("congestion_score").alias("avg_congestion"),
        count("*").alias("reading_count")
    )
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end",   col("window.end"))
    .withColumn("written_at",   current_timestamp())
    .drop("window")
)

# =========================
# AGGREGATION 2 — % of segments congested (KPI)
# =========================
# Per window: distinct xd_id with any reading vs distinct xd_id with at least one
# reading where congestion_score > CONGESTION_THRESHOLD (segment counts as
# congested if any observation in the window is over the threshold).
# CONGESTION_THRESHOLD = 0.3 means speed is 30% or more below reference speed.
# approx_count_distinct: cheaper than exact countDistinct on very large windows; KPI is approximate.

agg_network_pct = (
    enriched
    .groupBy(
        window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION)
    )
    .agg(
        approx_count_distinct("xd_id").alias("total_segments"),
        approx_count_distinct(
            when(col("congestion_score") > CONGESTION_THRESHOLD, col("xd_id"))
        ).alias("congested_segments")
    )
    .withColumn(
        "pct_congested",
        when(
            col("total_segments") > 0,
            col("congested_segments") / col("total_segments") * lit(100.0),
        ).otherwise(lit(None)),
    )
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end",   col("window.end"))
    .withColumn("written_at",   current_timestamp())
    .drop("window")
)


# =========================
# AGGREGATION 3 — Congestion by road type (frc)
# =========================
# Groups by frc within each window
# Shows whether interstates, highways, or local streets are more congested
# frc values: 1=interstate, 2=major highway, 3=arterial, 4=minor arterial, 5=local

agg_by_road_type = (
    enriched
    .groupBy(
        window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
        col("frc")
    )
    .agg(
        avg("speed").alias("avg_speed"),
        avg("congestion_score").alias("avg_congestion"),
        count("*").alias("reading_count")
    )
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end",   col("window.end"))
    .withColumn("written_at",   current_timestamp())
    .drop("window")
)

# =========================
# WRITE TO S3
# =========================

query_by_segment = (
    agg_by_segment
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", S3_BY_SEGMENT)
    .option("checkpointLocation", CHECKPOINT_BY_SEGMENT)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

query_network_pct = (
    agg_network_pct
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", S3_NETWORK_PCT)
    .option("checkpointLocation", CHECKPOINT_NETWORK_PCT)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

query_by_road_type = (
    agg_by_road_type
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", S3_BY_ROAD_TYPE)
    .option("checkpointLocation", CHECKPOINT_BY_ROAD_TYPE)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

# Keep all three queries running
spark.streams.awaitAnyTermination()