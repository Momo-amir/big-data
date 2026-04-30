"""
Standalone extract script — decoupled from Transform/Load.

In the real-time pipeline, run this independently to drop a CSV into
HDFS Input_dir. The background streaming process (transform_stream.py)
detects the new file and automatically runs T+L + Viz + Kafka.

Usage (inside etl-runner container):
    python extract_only.py
"""

from modules.extract import extract

CSV_URL = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/iris.csv"

hdfs_path = extract(CSV_URL)
print(f"[extract_only] Done. File available at hdfs:{hdfs_path}")
print("[extract_only] The streaming pipeline will pick it up automatically.")
