"""
Real-time pipeline — runs as a background process.

Monitors HDFS Input_dir using Spark Structured Streaming (readStream on HDFS).
When a new CSV file arrives (dropped by the extract script), Spark automatically:
  1. Reads and filters the file (Transform — Iris-setosa only).
  2. Saves encrypted CSV to HDFS Output_dir (Load).
  3. Saves encrypted table to Hive (Load).
  4. Generates scatter, histogram and boxplot diagrams on HDFS (Visualize).
  5. Streams the decrypted rows to Kafka (Produce).

Run from inside the etl-runner container:
    python -m modules.transform_stream
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from modules.security import encrypt
from modules.load import save_csv, save_to_hive
from modules.visualization import fetch_iris_data, scatter_plot, histogram, boxplot
from modules.kafka_producer import stream_to_kafka

SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
HDFS_INPUT_DIR = "hdfs://namenode:9000/data/Input_dir"
CHECKPOINT_DIR = "hdfs://namenode:9000/checkpoints/stream"
HIVE_WAREHOUSE = "hdfs://namenode:9000/user/hive/warehouse"
HIVE_METASTORE = "thrift://hive-metastore:9083"
IRIS_COLUMNS = ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]


def _get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IrisRealTimeETL")
        .master(SPARK_MASTER)
        .config("spark.sql.warehouse.dir", HIVE_WAREHOUSE)
        .config("hive.metastore.uris", HIVE_METASTORE)
        .config("hive.exec.scratchdir", "hdfs://namenode:9000/tmp/hive-scratch")
        .enableHiveSupport()
        .getOrCreate()
    )


def _process_batch(batch_df, batch_id: int, spark: SparkSession) -> None:
    """Called by Structured Streaming for each micro-batch of new files."""
    if batch_df.rdd.isEmpty():
        return

    print(f"[stream] Processing batch {batch_id} ...")

    # Rename if needed (schema comes from readStream header=True)
    if "species" not in batch_df.columns:
        batch_df = batch_df.toDF(*IRIS_COLUMNS)

    filtered = batch_df.filter(col("species") == "Iris-setosa")
    count = filtered.count()
    if count == 0:
        print(f"[stream] Batch {batch_id}: no Iris-setosa rows.")
        return

    print(f"[stream] Batch {batch_id}: {count} Iris-setosa rows — running Load, Viz, Kafka ...")

    save_csv(filtered, "iris.csv")
    save_to_hive(filtered, spark)

    iris_data = fetch_iris_data(spark)
    scatter_plot(iris_data)
    histogram(iris_data)
    boxplot(iris_data)

    stream_to_kafka(spark)
    print(f"[stream] Batch {batch_id} complete.")


IRIS_SCHEMA = StructType([
    StructField("sepal_length", DoubleType(), True),
    StructField("sepal_width",  DoubleType(), True),
    StructField("petal_length", DoubleType(), True),
    StructField("petal_width",  DoubleType(), True),
    StructField("species",      StringType(), True),
])


def run() -> None:
    spark = _get_spark()

    # Use a static schema so the stream can start before any file arrives
    # and without accidentally re-reading old files for schema inference.
    stream = (
        spark.readStream
        .schema(IRIS_SCHEMA)
        .option("header", "true")
        .option("maxFilesPerTrigger", 1)
        # latestFirst=false ensures oldest new file is processed first
        .option("latestFirst", "false")
        .csv(HDFS_INPUT_DIR)
    )

    query = (
        stream.writeStream
        .foreachBatch(lambda df, bid: _process_batch(df, bid, spark))
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("[stream] Watching HDFS Input_dir for new CSV files. Press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    run()
