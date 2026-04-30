"""
Load module — saves the transformed Spark DataFrame to:
  1. An encrypted CSV in HDFS Output_dir.
  2. An encrypted table in Apache Hive (via Spark's built-in Hive support).
"""

import io
import os
import csv
import requests
from pyspark.sql import DataFrame

from modules.security import encrypt

HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_OUTPUT_DIR = "/data/Output_dir"
HIVE_DB = os.getenv("HIVE_DB", "irisdb")
HIVE_TABLE = os.getenv("HIVE_TABLE", "iris_setosa")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hdfs_write(hdfs_path: str, data: bytes) -> None:
    """PUT bytes directly to HDFS via WebHDFS."""
    create_url = (
        f"{HDFS_URL}/webhdfs/v1{hdfs_path}"
        f"?op=CREATE&overwrite=true&user.name=root"
    )
    redirect = requests.put(create_url, allow_redirects=False, timeout=10)
    if redirect.status_code != 307:
        raise RuntimeError(
            f"WebHDFS CREATE did not redirect (HTTP {redirect.status_code}): "
            f"{redirect.text}"
        )
    put_resp = requests.put(
        redirect.headers["Location"],
        data=data,
        headers={"Content-Type": "application/octet-stream"},
        timeout=30,
    )
    put_resp.raise_for_status()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def save_csv(df: DataFrame, source_filename: str) -> str:
    """
    Encrypt each cell of `df` with AES-CBC and write a CSV to HDFS Output_dir.
    The output filename is  transformed_<source_filename>.

    Returns the HDFS path of the written file.
    """
    out_filename = f"transformed_{source_filename}"
    hdfs_path = f"{HDFS_OUTPUT_DIR}/{out_filename}"

    rows = df.collect()
    columns = df.columns

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    for row in rows:
        writer.writerow([encrypt(str(v)) if v is not None else "" for v in row])

    _hdfs_write(hdfs_path, buf.getvalue().encode("utf-8"))
    print(f"[load] Encrypted CSV saved → hdfs:{hdfs_path}")
    return hdfs_path


def save_to_hive(df: DataFrame, spark) -> None:
    """
    Save the transformed (unencrypted) DataFrame to Hive as an encrypted table.
    Each string cell is encrypted with AES-CBC before storing.

    Hive database is auto-created if it doesn't exist.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
    spark.sql(f"USE {HIVE_DB}")

    # Collect, encrypt every value, rebuild as a local Python structure,
    # then create a new Spark DF and write it to Hive.
    columns = df.columns
    rows = df.collect()

    encrypted_rows = [
        [encrypt(str(v)) if v is not None else "" for v in row]
        for row in rows
    ]

    enc_df = spark.createDataFrame(encrypted_rows, schema=columns)

    enc_df.write \
        .mode("overwrite") \
        .format("hive") \
        .saveAsTable(f"{HIVE_DB}.{HIVE_TABLE}")

    print(f"[load] Encrypted table saved → Hive {HIVE_DB}.{HIVE_TABLE}")
