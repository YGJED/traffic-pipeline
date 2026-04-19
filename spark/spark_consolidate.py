from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
S3_BUCKET = os.getenv("S3_BUCKET", "ndot-traffic-pipeline")
HISTORICAL_S3A = f's3a://{S3_BUCKET}/historical'

spark = (
    SparkSession.builder
    .appName("traffic-consolidate")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.session.token", AWS_SESSION_TOKEN)
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.executor.instances", "3")
    .config("spark.executor.cores", "2")
    .config("spark.cores.max", "6")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "1g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

YEAR = int(os.getenv("SPARK_YEAR", "2023"))

AGG_TYPES = ["by_hour", "by_day_of_week", "by_road_type", "by_direction", "top_segments"]

_hist = HISTORICAL_S3A.rstrip("/")

for agg in AGG_TYPES:
    src = f"{_hist}/{agg}/year={YEAR}"
    print(f"Consolidating {agg} for year={YEAR} from {src}")
    df = spark.read.parquet(src).cache()
    df.count()
    df.coalesce(1).write.mode("overwrite").parquet(src)
    df.unpersist()
    print(f"Done: {agg}")

spark.stop()
