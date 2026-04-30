"""
Batch Transform — reads iris.csv from HDFS, filters for Iris-setosa,
returns a Spark DataFrame. Called from batch main.py.
"""

import os
from pyspark.sql import SparkSession, DataFrame

SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
IRIS_COLUMNS = ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]


HIVE_WAREHOUSE = "hdfs://namenode:9000/user/hive/warehouse"
HIVE_METASTORE = "thrift://hive-metastore:9083"


def get_spark(app_name: str = "IrisBatchETL") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.sql.warehouse.dir", HIVE_WAREHOUSE)
        .config("hive.metastore.uris", HIVE_METASTORE)
        # Scratch dir must also be on HDFS so executors can write to it
        .config("hive.exec.scratchdir", "hdfs://namenode:9000/tmp/hive-scratch")
        .enableHiveSupport()
        .getOrCreate()
    )


def transform(hdfs_input_path: str, spark: SparkSession = None) -> DataFrame:
    """
    Read the CSV at `hdfs_input_path` (on HDFS) and return only Iris-setosa rows.
    The file already has a header row added by the extract module.
    """
    if spark is None:
        spark = get_spark()

    df = spark.read.csv(hdfs_input_path, header=True, inferSchema=True)

    # Rename columns in case header was missing (safety net)
    if "species" not in df.columns:
        df = df.toDF(*IRIS_COLUMNS)

    filtered = df.filter(df["species"] == "Iris-setosa")
    print(f"[transform_batch] {filtered.count()} Iris-setosa rows kept")
    return filtered
