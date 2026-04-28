"""
Extract phase of the ETL pipeline.

Fetches air quality measurements from the OpenAQ API over HTTPS
and writes the raw JSON response directly to HDFS using streaming
chunked download (8 KB at a time — never loads the full response in memory).

Security measures:
  - HTTPS enforced via urlparse check before any request is made
  - Query parameters passed as dict to requests (no raw string interpolation)
  - Input validated against strict allowlists before use
  - subprocess calls use list form with shell=False (see submit_jobs.py)
"""

import logging
import os
import sys
from datetime import date

import requests

from utils.hdfs_client import CHUNK_SIZE, connect, ensure_dir, write_stream
from utils.validation import build_safe_params, enforce_https

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def extract(url: str, params: dict, hdfs_client, hdfs_path: str) -> int:
    today = date.today().isoformat()
    dest = f"{hdfs_path}/{today}/measurements.json"
    ensure_dir(hdfs_client, f"{hdfs_path}/{today}")

    session = requests.Session()
    with session.get(url, params=params, stream=True, timeout=30) as response:
        response.raise_for_status()
        logger.info("Streaming response from %s (status %d)", response.url, response.status_code)
        total = write_stream(hdfs_client, dest, response.iter_content(chunk_size=CHUNK_SIZE))

    return total


def main() -> int:
    url = os.environ.get("OPENAQ_URL", "https://api.openaq.org/v2/measurements")
    country = os.environ.get("OPENAQ_COUNTRY", "DK")
    limit = int(os.environ.get("OPENAQ_LIMIT", "1000"))
    hdfs_url = os.environ.get("HDFS_URL", "http://namenode:9870")
    hdfs_raw_path = "/data/raw/openaq"

    try:
        enforce_https(url)
        params = build_safe_params(country, limit)
    except ValueError as exc:
        logger.error("Parameter validation failed: %s", exc)
        return 1

    client = connect(hdfs_url)

    try:
        bytes_written = extract(url, params, client, hdfs_raw_path)
        logger.info("Extract complete. %d bytes written to HDFS.", bytes_written)
        return 0
    except requests.HTTPError as exc:
        logger.error("HTTP error during extract: %s", exc)
        return 1
    except Exception as exc:
        logger.error("Unexpected error during extract: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
