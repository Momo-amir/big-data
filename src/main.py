import os
import requests
from pyspark.sql import SparkSession


HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_UI = "http://spark-master:8080"

print("Hello from Python!")

r = requests.get(f"{HDFS_URL}/webhdfs/v1/?op=LISTSTATUS", timeout=10)
print(f"Hello from Hadoop! (HTTP {r.status_code})")

r = requests.get(f"{SPARK_UI}/json/", timeout=10)
print(f"Hello from Spark UI! (HTTP {r.status_code})")

spark = SparkSession.builder \
    .appName("HelloSpark") \
    .master(SPARK_MASTER_URL) \
    .getOrCreate()

print(f"Connected to Spark master: {spark.sparkContext.master}")

df = spark.createDataFrame(
    [("momo", 1), ("amer", 2)],
    ["name", "id"]
)

df.show()

spark.stop()
