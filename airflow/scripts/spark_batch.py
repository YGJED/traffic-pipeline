from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, avg

import os
from dotenv import load_dotenv

# Same as upload script: put AWS_* in a project `.env`; load_dotenv() fills the env for this process.
load_dotenv("/opt/airflow/.env")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

# Curated aggregation outputs (S3 console: s3://ndot-traffic-pipeline/historical/...)
HISTORICAL_S3A = "s3a://ndot-traffic-pipeline/historical"

spark = (
    SparkSession.builder
    .appName("traffic-batch")
    # NoSuchMethodError in VectoredReadUtils while reading Parquet from S3A: Ivy pulled an older hadoop-aws
    # than Spark’s embedded Hadoop, so classes didn’t line up. Match the minor version (e.g. 3.4.2 for PySpark 4.1.x).
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.session.token", AWS_SESSION_TOKEN)
    # S3A 403 / wrong identity with STS or assumed-role creds: the static key provider ignores AWS_SESSION_TOKEN.
    # TemporaryAWSCredentialsProvider sends access key + secret + session token together.
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    # NumberFormatException during S3A init: some fs.s3a.* defaults are human durations ("60s", "24h") but the parser
    # expects a plain number (seconds/ms). Setting explicit numeric strings avoids that path.
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
    # UnsatisfiedLinkError NativeIO$Windows.access0 on Parquet write to S3A: default upload buffer writes temp
    # blocks to local disk and hits Hadoop’s Windows JNI; PySpark often has no working native lib. bytebuffer stays in-heap.
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
    .getOrCreate()
)


# Suppress verbose Spark logs so we can see our output
spark.sparkContext.setLogLevel("ERROR")


# Read from S3 (data was uploaded with upload_parquets.py); hive-style partitions under raw/year=2023/month=*.
df_inrix = spark.read.parquet(
    "s3a://ndot-traffic-pipeline/raw/year=2023"
)

# Join lookup file in raw/ on the same bucket (CSV, not Parquet).
df_xd = spark.read.csv(
    "s3a://ndot-traffic-pipeline/raw/XD_Identification.csv",
    header = True,
    inferSchema = True
)

# Join the two dataframes on the XD_ID column
df_joined = df_inrix.join(df_xd, df_inrix.xd_id == df_xd.xd, "inner")

# Drop the duplicate xd column from the XD table since we already have xd_id
df_joined = df_joined.drop("xd")

# Compute congestion score per row: how congested relative to free flow speed
# 0 = no congestion, 1 = completely stopped
df_joined = df_joined.withColumn(
    "congestion_score",
    (col("reference_speed") - col("speed")) / col("reference_speed")
)


# Aggregation 1: Average speed by hour of day (0-23)
# Extracts the hour from the timestamp, groups all readings by that hour across the full year
# and computes the average speed and congestion score for each hour
# Result tells us: which hours of the day are most congested across all Nashville roads
print("Computing aggregation: by_hour")
df_by_hr= df_joined.groupBy(hour(col("measurement_tstamp")).alias("hour")) \
    .agg(   
        avg("speed").alias("avg_speed"),
        avg("congestion_score").alias("avg_congestion_score")
    ) \
    .orderBy("hour")

# Aggregation 2: Average speed by day of week (1-7, where 1 is Sunday)
# Extracts the day of week from the timestamp, groups all readings by that day across the full year
# and computes the average speed and congestion score for each day
# Result tells us: which days of the week are most congested across all Nashville roads
# 0 = no congestion, 1 = completely stopped

print("Computing aggregation: by_day_of_week")
df_by_day = df_joined.groupBy(dayofweek(col("measurement_tstamp")).alias("day_of_week")) \
    .agg(   
        avg("speed").alias("avg_speed"),
        avg("congestion_score").alias("avg_congestion_score")
    ) \
    .orderBy("day_of_week")

# Aggregation 3: Average congestion by road type (frc)
# frc = functional road class: 1=interstate, 2=major highway, 3=arterial, 4=minor arterial, 5=local street
# Groups all readings by road type and computes average speed and congestion score
# Result tells us: which types of roads are most congested across Nashville
print("Computing aggregation: by_road_type")
df_by_road_type = df_joined.groupBy(col("frc").alias("road_type")) \
    .agg(
        avg("speed").alias("avg_speed"),
        avg("congestion_score").alias("avg_congestion_score")
    ) \
    .orderBy("road_type")

# Aggregation 4: Average speed by direction (bearing)
# bearing = cardinal direction traffic flows on that segment (N/S/E/W)
# Groups all readings by direction and computes average speed and congestion score
# Result tells us: which directions of travel are most congested across Nashville
print("Computing aggregation: by_direction")
df_by_direction = df_joined.groupBy(col("bearing").alias("direction")) \
    .agg( 
        avg("speed").alias("avg_speed"),
        avg("congestion_score").alias("avg_congestion_score")
    ) \
    .orderBy("direction")


# Aggregation 5: Worst segments overall (top 20)
# Groups by xd_id (unique segment ID) rather than road-name because roads are made up of many
# segments that can have very different traffic conditions. Grouping by road-name would average
# all segments together and mask localized bottlenecks — a single bad segment on I-65 could be
# hidden by the many fine segments around it. Grouping by xd_id gives precise, segment-level
# ranking so we can identify the exact stretches of road that are consistently worst.
# road-name, bearing, and frc are carried along purely for display purposes on the dashboard.
print("Computing aggregation: top_segments")
df_by_segment = df_joined.groupBy("xd_id", "road-name", "bearing", "frc") \
    .agg(
        avg("congestion_score").alias("avg_congestion"),
        avg("speed").alias("avg_speed")
    ) \
    .orderBy(col("avg_congestion").desc()) \
    .limit(20)


# Write curated Parquet back to the same bucket (S3A); fast.upload.buffer=bytebuffer above avoids Windows write JNI failures.
_hist = HISTORICAL_S3A.rstrip("/")
print(f"Writing aggregation output: s3://ndot-traffic-pipeline/historical/by_hour")
df_by_hr.write.mode("overwrite").parquet(f"{_hist}/by_hour")
print("Done writing: by_hour")

print(f"Writing aggregation output: s3://ndot-traffic-pipeline/historical/by_day_of_week")
df_by_day.write.mode("overwrite").parquet(f"{_hist}/by_day_of_week")
print("Done writing: by_day_of_week")

print(f"Writing aggregation output: s3://ndot-traffic-pipeline/historical/by_road_type")
df_by_road_type.write.mode("overwrite").parquet(f"{_hist}/by_road_type")
print("Done writing: by_road_type")

print(f"Writing aggregation output: s3://ndot-traffic-pipeline/historical/by_direction")
df_by_direction.write.mode("overwrite").parquet(f"{_hist}/by_direction")
print("Done writing: by_direction")

print(f"Writing aggregation output: s3://ndot-traffic-pipeline/historical/top_segments")
df_by_segment.write.mode("overwrite").parquet(f"{_hist}/top_segments")
print("Done writing: top_segments")
print("Batch aggregation job complete.")

# Stop the Spark session
spark.stop()