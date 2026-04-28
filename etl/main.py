"""
ETL entrypoint: runs Extract then triggers the Spark Transform+Load jobs.
"""

import logging
import sys
import time

import extract
import submit_jobs
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    logger.info("=== ETL Pipeline Start ===")

    logger.info("--- Extract ---")
    rc = extract.main()
    if rc != 0:
        logger.error("Extract phase failed.")
        return rc

    # Give HDFS a moment to flush before Spark reads
    time.sleep(3)

    logger.info("--- Transform + Load (Batch) ---")
    today = date.today().isoformat()
    hdfs_input = f"hdfs://namenode:9000/data/raw/openaq/{today}"
    hdfs_output = f"hdfs://namenode:9000/data/processed/openaq/{today}"

    try:
        submit_jobs.submit_batch(hdfs_input, hdfs_output)
    except Exception as exc:
        logger.error("Transform/Load phase failed: %s", exc)
        return 1

    logger.info("=== ETL Pipeline Complete ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
