"""
Transform + Load phase (batch).

Reads raw JSON from HDFS, cleans and filters with PySpark,
then writes the result as CSV directly back to HDFS.

Usage:
  spark-submit transform_batch.py --input <hdfs_input_path> --output <hdfs_output_path>
"""

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
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

RAW_SCHEMA = StructType([
    StructField("results", StructType([
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("parameter", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("date", StructType([
            StructField("utc", StringType(), True),
        ]), True),
    ]), True),
])

FLAT_SCHEMA = StructType([
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
        .appName("openaq-batch-transform")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def load_raw(spark: SparkSession, hdfs_path: str) -> DataFrame:
    """
    Reads the raw JSON dumped by the extract phase.
    OpenAQ wraps results in a top-level 'results' array.
    """
    raw = spark.read.option("multiline", "true").json(hdfs_path)

    if "results" not in raw.columns:
        raise ValueError(f"Expected 'results' column in JSON at {hdfs_path}")

    return raw.select(F.explode("results").alias("r")).select(
        F.col("r.value").cast(DoubleType()).alias("value"),
        F.col("r.unit").alias("unit"),
        F.col("r.parameter").alias("parameter"),
        F.col("r.country").alias("country"),
        F.col("r.city").alias("city"),
        F.col("r.date.utc").alias("measured_at"),
    )


def clean(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("value").isNotNull())
        .filter(F.col("value") >= 0)
        .filter(F.col("unit").isin(list(ALLOWED_UNITS)))
        .withColumn("unit", F.lower(F.col("unit")))
        .withColumn("measured_at", F.to_timestamp("measured_at"))
        .withColumn("parameter", F.lower(F.col("parameter")))
        .dropDuplicates()
    )


def filter_recent(df: DataFrame, days: int = 7) -> DataFrame:
    cutoff = F.date_sub(F.current_date(), days)
    return df.filter(F.col("measured_at") >= cutoff)


def save_csv(df: DataFrame, hdfs_output: str) -> None:
    """
    Writes transformed data as CSV directly to HDFS.
    Partitioned by country and parameter for efficient downstream access.
    """
    (
        df.write
        .mode("overwrite")
        .option("header", "true")
        .partitionBy("country", "parameter")
        .csv(hdfs_output)
    )
    logger.info("Saved CSV to HDFS: %s", hdfs_output)


def main(hdfs_input: str, hdfs_output: str) -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Loading raw data from: %s", hdfs_input)
    df = load_raw(spark, hdfs_input)

    logger.info("Cleaning and filtering...")
    df = clean(df)
    df = filter_recent(df, days=7)

    count = df.count()
    logger.info("Transformed row count: %d", count)

    logger.info("Saving CSV to: %s", hdfs_output)
    save_csv(df, hdfs_output)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch transform for OpenAQ data")
    parser.add_argument("--input", required=True, help="HDFS path to raw JSON")
    parser.add_argument("--output", required=True, help="HDFS path for output CSV")
    args = parser.parse_args()
    main(args.input, args.output)
