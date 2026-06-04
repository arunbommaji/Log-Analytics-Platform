import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RAW_LOGS    = os.getenv("KAFKA_TOPIC_RAW_LOGS", "raw-logs")
