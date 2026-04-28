"""
Transform + Load phase (streaming / real-time).

Watches an HDFS directory for new JSON files dropped by the extract phase
and processes each file as it arrives using Spark Structured Streaming.
Output is appended as CSV to HDFS.

This simulates a near-real-time Lambda Architecture on the same cluster
as the batch job, using identical cleaning logic.

Usage:
  spark-submit transform_stream.py \
    --watch <hdfs_watch_path> \
    --output <hdfs_output_path> \
    --checkpoint <hdfs_checkpoint_dir>
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RESULT_SCHEMA = StructType([
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("parameter", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("measured_at", StringType(), True),
])

ALLOWED_UNITS = {"µg/m³", "ppm", "ppb", "mg/m³"}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("openaq-stream-transform")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def main(hdfs_watch: str, hdfs_output: str, checkpoint_dir: str) -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("json")
        .option("multiline", "true")
        .option("maxFilesPerTrigger", 1)
        .load(hdfs_watch)
    )

    if "results" in raw_stream.columns:
        flat = raw_stream.select(F.explode("results").alias("r")).select(
            F.col("r.value").cast(DoubleType()).alias("value"),
            F.col("r.unit").alias("unit"),
            F.col("r.parameter").alias("parameter"),
            F.col("r.country").alias("country"),
            F.col("r.city").alias("city"),
            F.col("r.date.utc").alias("measured_at"),
        )
    else:
        flat = raw_stream

    cleaned = (
        flat
        .filter(F.col("value").isNotNull())
        .filter(F.col("value") >= 0)
        .filter(F.col("unit").isin(list(ALLOWED_UNITS)))
        .withColumn("unit", F.lower(F.col("unit")))
        .withColumn("measured_at", F.to_timestamp("measured_at"))
        .withColumn("parameter", F.lower(F.col("parameter")))
    )

    query = (
        cleaned.writeStream
        .format("csv")
        .option("path", hdfs_output)
        .option("checkpointLocation", checkpoint_dir)
        .option("header", "true")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Streaming query started. Watching: %s", hdfs_watch)
    query.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Streaming transform for OpenAQ data")
    parser.add_argument("--watch", required=True, help="HDFS path to watch for new JSON files")
    parser.add_argument("--output", required=True, help="HDFS path for streaming CSV output")
    parser.add_argument("--checkpoint", required=True, help="HDFS path for Spark checkpoint data")
    args = parser.parse_args()
    main(args.watch, args.output, args.checkpoint)
