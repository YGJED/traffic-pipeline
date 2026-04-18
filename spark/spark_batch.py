from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, avg, year, month, row_number
from pyspark.sql.window import Window

import os
from dotenv import load_dotenv

# Same as upload script: put AWS_* in a project `.env`; load_dotenv() fills the env for this process.
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

S3_BUCKET = os.getenv("S3_BUCKET","ndot-traffic-pipeline")

# Curated aggregation outputs (S3 console: s3://ndot-traffic-pipeline/historical/...)
HISTORICAL_S3A = f's3a://{S3_BUCKET}/historical'

spark = (
    SparkSession.builder
    .appName("traffic-batch")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.session.token", AWS_SESSION_TOKEN)
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
    .config("spark.executor.memory", "1g")
    .config("spark.driver.memory", "1g")
    .config("spark.executor.cores", "2")
    .config("spark.cores.max", "2")
    .config("spark.executor.instances", "1")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()
)


# Suppress verbose Spark logs so we can see our output
spark.sparkContext.setLogLevel("ERROR")

def process_year(year_num):
    print(f"Processing year {year_num}")

    df_inrix = spark.read.parquet(
        f's3a://{S3_BUCKET}/raw/year={year_num}'
    )

    df_xd = spark.read.csv(
        f's3a://{S3_BUCKET}/raw/XD_Identification.csv',
        header=True,
        inferSchema=True
    )

    df_joined = df_inrix.join(df_xd, df_inrix.xd_id == df_xd.xd, "inner").drop("xd")

    df_joined = df_joined.withColumn(
        "congestion_score",
        (col("reference_speed") - col("speed")) / col("reference_speed")
    )    

    df_joined = df_joined.cache()
    df_joined.count()

    # Aggregation 1: Average speed by hour of day (0-23), grouped by year and month
    df_by_hr = df_joined.groupBy(
        year(col("measurement_tstamp")).alias("year"),
        month(col("measurement_tstamp")).alias("month"),
        hour(col("measurement_tstamp")).alias("hour")
    ).agg(
        avg("speed").alias("avg_speed"),
        avg("congestion_score").alias("avg_congestion_score")
    ).orderBy("year", "month", "hour")

    # Aggregation 2: Average speed by day of week (1-7, where 1 is Sunday), grouped by year and month
    df_by_day = df_joined.groupBy(
        year(col("measurement_tstamp")).alias("year"),
        month(col("measurement_tstamp")).alias("month"),
        dayofweek(col("measurement_tstamp")).alias("day_of_week")
    ).agg(
        avg("speed").alias("avg_speed"),
        avg("congestion_score").alias("avg_congestion_score")
    ).orderBy("year", "month", "day_of_week")

    # Aggregation 3: Average congestion by road type (frc), grouped by year and month
    df_by_road_type = df_joined.groupBy(
        year(col("measurement_tstamp")).alias("year"),
        month(col("measurement_tstamp")).alias("month"),
        col("frc").alias("road_type")
    ).agg(
        avg("speed").alias("avg_speed"),
        avg("congestion_score").alias("avg_congestion_score")
    ).orderBy("year", "month", "road_type")

    # Aggregation 4: Average speed by direction (bearing), grouped by year and month
    df_by_direction = df_joined.groupBy(
        year(col("measurement_tstamp")).alias("year"),
        month(col("measurement_tstamp")).alias("month"),
        col("bearing").alias("direction")
    ).agg(
        avg("speed").alias("avg_speed"),
        avg("congestion_score").alias("avg_congestion_score")
    ).orderBy("year", "month", "direction")

    # Aggregation 5: Worst segments overall (top 20), grouped by year and month
    df_by_segment = df_joined.groupBy(
        year(col("measurement_tstamp")).alias("year"),
        month(col("measurement_tstamp")).alias("month"),
        "xd_id", "road-name", "bearing", "frc"
    ).agg(
        avg("congestion_score").alias("avg_congestion"),
        avg("speed").alias("avg_speed")
    ).orderBy("year", "month", col("avg_congestion").desc())
    df_by_segment = df_by_segment.withColumn("rank", 
        row_number().over(
            Window.partitionBy("year", "month").orderBy(col("avg_congestion").desc())
        )
    ).filter(col("rank") <= 20)

    _hist = HISTORICAL_S3A.rstrip("/")

    df_by_hr.coalesce(1).write.mode("overwrite").parquet(f"{_hist}/by_hour/year={year_num}")
    df_by_day.coalesce(1).write.mode("overwrite").parquet(f"{_hist}/by_day_of_week/year={year_num}")
    df_by_road_type.coalesce(1).write.mode("overwrite").parquet(f"{_hist}/by_road_type/year={year_num}")
    df_by_direction.coalesce(1).write.mode("overwrite").parquet(f"{_hist}/by_direction/year={year_num}")
    df_by_segment.coalesce(1).write.mode("overwrite").parquet(f"{_hist}/top_segments/year={year_num}")

YEAR = 2023
process_year(YEAR)

# Stop the Spark session
spark.stop()