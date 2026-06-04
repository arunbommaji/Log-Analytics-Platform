import json
import logging
from kafka import KafkaProducer
from src.config import KAFKA_BOOTSTRAP_SERVERS

log = logging.getLogger(__name__)

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

def publish_log(producer: KafkaProducer, topic: str, log_entry: dict) -> None:
    producer.send(topic, key=log_entry.get("service"), value=log_entry)
    producer.flush()
