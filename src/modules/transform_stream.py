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

from modules.security import encrypt
from modules.load import save_csv, save_to_hive
from modules.visualization import fetch_iris_data, scatter_plot, histogram, boxplot
from modules.kafka_producer import stream_to_kafka

SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
HDFS_INPUT_DIR = "hdfs://namenode:9000/data/Input_dir"
CHECKPOINT_DIR = "hdfs://namenode:9000/checkpoints/stream"
IRIS_COLUMNS = ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]


def _get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IrisRealTimeETL")
        .master(SPARK_MASTER)
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


def run() -> None:
    spark = _get_spark()

    schema = (
        spark.read
        .csv(f"{HDFS_INPUT_DIR}/iris.csv", header=True, inferSchema=True)
        .schema
    )

    stream = (
        spark.readStream
        .schema(schema)
        .option("header", "true")
        .option("maxFilesPerTrigger", 1)
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
