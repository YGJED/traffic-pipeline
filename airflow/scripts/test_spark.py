from pyspark.sql import SparkSession

# SparkSession is the entry point to everything PySpark
# You always need to create one before doing anything with Spark
# Think of it like opening a connection to a database before querying it
spark = SparkSession.builder \
    .appName("traffic-test") \
    .getOrCreate()

print("PySpark is working!")

# Always stop the session when done to free up memory/resources
spark.stop()