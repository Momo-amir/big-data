"""
Kafka producer — reads the encrypted Hive table, decrypts each row,
and publishes it as a JSON message to the configured Kafka topic.
"""

import json
import os
from kafka import KafkaProducer
from pyspark.sql import SparkSession

from modules.security import decrypt

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iris-setosa")
HIVE_DB = os.getenv("HIVE_DB", "irisdb")
HIVE_TABLE = os.getenv("HIVE_TABLE", "iris_setosa")


def stream_to_kafka(spark: SparkSession) -> None:
    """
    Read the Hive table (encrypted), decrypt every cell, and send each row
    as a JSON message to Kafka. Consumers receive plaintext data.
    """
    spark.sql(f"USE {HIVE_DB}")
    df = spark.sql(f"SELECT * FROM {HIVE_TABLE}")
    columns = df.columns
    rows = df.collect()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sent = 0
    for row in rows:
        record = {col: decrypt(str(row[i])) for i, col in enumerate(columns)}
        producer.send(KAFKA_TOPIC, value=record)
        sent += 1

    producer.flush()
    producer.close()
    print(f"[kafka_producer] Streamed {sent} records → topic '{KAFKA_TOPIC}'")
