"""
Batch ETL entrypoint — Extract → Transform → Load → Visualize → Kafka.

Usage (inside etl-runner container):
    python main.py

Environment variables (set in docker-compose or .env):
    HDFS_URL, SPARK_MASTER_URL, HIVE_DB, HIVE_TABLE,
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, AES_KEY
"""

from modules.extract import extract
from modules.transform_batch import get_spark, transform
from modules.load import save_csv, save_to_hive
from modules.visualization import fetch_iris_data, scatter_plot, histogram, boxplot
from modules.kafka_producer import stream_to_kafka

CSV_URL = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/iris.csv"

# --- Extract ---
hdfs_input_path = extract(CSV_URL)
print(f"[main] Extract complete: {hdfs_input_path}")

# --- Transform (batch) ---
spark = get_spark()
filtered_df = transform(f"hdfs://namenode:9000{hdfs_input_path}", spark)

# --- Load ---
save_csv(filtered_df, "iris.csv")
save_to_hive(filtered_df, spark)

# --- Visualize ---
data = fetch_iris_data(spark)
scatter_plot(data)
histogram(data)
boxplot(data)

# --- Stream to Kafka ---
stream_to_kafka(spark)

spark.stop()
print("[main] Batch ETL pipeline complete.")
