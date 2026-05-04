"""
Standalone Kafka consumer — run this independently to receive streamed
Iris-setosa records from the big data pipeline.

Usage (inside etl-runner container or any host with Kafka access):
    python consumer.py
"""

import json
import os
import sys
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iris-setosa")
# No persistent group — each run replays all messages from offset 0,
# which is what we want for a live demo (and for --auto_offset_reset=earliest to apply).
KAFKA_GROUP = os.getenv("KAFKA_GROUP_ID", None)


def main() -> None:
    print(f"[consumer] Connecting to Kafka at {KAFKA_BOOTSTRAP} ...")
    print(f"[consumer] Subscribing to topic '{KAFKA_TOPIC}'. Waiting for messages (Ctrl+C to stop)...\n")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    try:
        for message in consumer:
            record = message.value
            print(
                f"[consumer] partition={message.partition} offset={message.offset} | "
                f"sepal_length={record.get('sepal_length')}  "
                f"sepal_width={record.get('sepal_width')}  "
                f"petal_length={record.get('petal_length')}  "
                f"petal_width={record.get('petal_width')}  "
                f"species={record.get('species')}"
            )
    except KeyboardInterrupt:
        print("\n[consumer] Stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
