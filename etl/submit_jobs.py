"""
Submits PySpark jobs to the Spark cluster via spark-submit.

All subprocess calls use a list (shell=False) — never shell=True.
This prevents shell injection even if HDFS paths contain special characters.
"""

import logging
import subprocess
import sys
from datetime import date

logger = logging.getLogger(__name__)

SPARK_MASTER = "spark://spark-master:7077"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"


def _submit(script: str, extra_args: list[str]) -> None:
    cmd = [
        SPARK_SUBMIT,
        "--master", SPARK_MASTER,
        "--deploy-mode", "client",
        script,
        *extra_args,
    ]
    logger.info("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True, shell=False)


def submit_batch(hdfs_input: str, hdfs_output: str) -> None:
    _submit(
        "/opt/spark/jobs/transform_batch.py",
        ["--input", hdfs_input, "--output", hdfs_output],
    )


def submit_stream(hdfs_watch: str, hdfs_output: str, checkpoint: str) -> None:
    _submit(
        "/opt/spark/jobs/transform_stream.py",
        ["--watch", hdfs_watch, "--output", hdfs_output, "--checkpoint", checkpoint],
    )


if __name__ == "__main__":
    today = date.today().isoformat()
    hdfs_input = f"hdfs://namenode:9000/data/raw/openaq/{today}"
    hdfs_output = f"hdfs://namenode:9000/data/processed/openaq/{today}"
    hdfs_stream_out = "hdfs://namenode:9000/data/stream/openaq"
    hdfs_checkpoint = "hdfs://namenode:9000/checkpoints/openaq"

    try:
        submit_batch(hdfs_input, hdfs_output)
        logger.info("Batch job submitted successfully.")
    except subprocess.CalledProcessError as exc:
        logger.error("Batch job failed: %s", exc)
        sys.exit(1)
