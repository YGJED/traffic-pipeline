from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, avg, year, month, row_number, broadcast
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

# spark = (
#     SparkSession.builder
#     .appName("traffic-batch")
#     # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
#     .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
#     .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
#     .config("spark.hadoop.fs.s3a.session.token", AWS_SESSION_TOKEN)
#     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
#     .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
#     .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
#     .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
#     .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
#     .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
#     .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
#     .config("spark.executor.memory", "1g")
#     .config("spark.driver.memory", "1g")
#     .config("spark.executor.cores", "6")
#     .config("spark.cores.max", "6")
#     .config("spark.executor.instances", "1")
#     .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
#     .getOrCreate()
# )

spark = (
    SparkSession.builder
    .appName("traffic-batch")
    # S3 Credentials
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.session.token", AWS_SESSION_TOKEN)
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    
    # Cluster Resource Control
    .config("spark.executor.instances", "3")    # Request 3 separate workers
    .config("spark.executor.cores", "2")        # Each worker uses 2 cores
    .config("spark.cores.max", "6")             # Cap total cluster usage at 6 cores
    .config("spark.executor.memory", "2g")      # 2GB RAM per worker
    .config("spark.driver.memory", "1g")
    
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)


# Suppress verbose Spark logs so we can see our output
spark.sparkContext.setLogLevel("INFO")

def process_year(year_num):
    print(f"Processing year {year_num}")
    print(f"AWS access key ID {AWS_ACCESS_KEY_ID}")

    df_inrix = spark.read.parquet(
        f's3a://{S3_BUCKET}/raw/year={year_num}'
    )

    print("Reead DF INRIX Sucessfully")

    df_xd = spark.read.csv(
        f's3a://{S3_BUCKET}/raw/XD_Identification.csv',
        header=True,
        inferSchema=True
    )

    df_joined = df_inrix.join(broadcast(df_xd), df_inrix.xd_id == df_xd.xd, "inner").drop("xd")

    df_joined = df_joined.withColumn(
        "congestion_score",
        (col("reference_speed") - col("speed")) / col("reference_speed")
    )    

    df_joined = df_joined.cache()
    df_joined.count()
    print("Joined dataframes. Starting aggregations")

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

def process_month(month_num, year_num):
    print(f"Processing month {month_num}")

    df_inrix = spark.read.parquet(
        f's3a://{S3_BUCKET}/raw/year={year_num}/month={month_num}'
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
            Window.orderBy(col("avg_congestion").desc())
        )
    ).filter(col("rank") <= 20)

    _hist = HISTORICAL_S3A.rstrip("/")
    df_by_hr.write.mode("overwrite").partitionBy("year", "month").parquet(f"{_hist}/by_hour")
    df_by_day.write.mode("overwrite").partitionBy("year", "month").parquet(f"{_hist}/by_day_of_week")
    df_by_road_type.write.mode("overwrite").partitionBy("year", "month").parquet(f"{_hist}/by_road_type")
    df_by_direction.write.mode("overwrite").partitionBy("year", "month").parquet(f"{_hist}/by_direction")
    df_by_segment.write.mode("overwrite").partitionBy("year", "month").parquet(f"{_hist}/top_segments")

YEAR = 2023
process_year(YEAR)

# Stop the Spark session
spark.stop()