import json
import logging
import time
from kafka import KafkaConsumer
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
import redis as Redis
from src.config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RAW_LOGS,
    KAFKA_TOPIC_INCIDENTS, KAFKA_CONSUMER_GROUP,
)
from src.es_client import create_es_client, ensure_index, index_log
from src.incident_detector import create_redis, detect

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def wait_for_kafka(servers: str, retries: int = 15, delay: int = 5) -> None:
    from kafka.admin import KafkaAdminClient
    for i in range(retries):
        try:
            client = KafkaAdminClient(bootstrap_servers=servers, request_timeout_ms=3000)
            client.close()
            log.info("Kafka is ready")
            return
        except Exception as e:
            log.info("Waiting for Kafka (%d/%d): %s", i+1, retries, e)
            time.sleep(delay)
    raise RuntimeError("Kafka not reachable after retries")


def wait_for_es(es: Elasticsearch, retries: int = 15, delay: int = 5) -> None:
    for i in range(retries):
        try:
            if es.ping():
                log.info("Elasticsearch is ready")
                return
        except Exception:
            pass
        log.info("Waiting for Elasticsearch (%d/%d)…", i+1, retries)
        time.sleep(delay)
    raise RuntimeError("Elasticsearch not reachable after retries")


def main():
    wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

    es = create_es_client()
    wait_for_es(es)
    ensure_index(es)

    r = create_redis()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    consumer = KafkaConsumer(
        KAFKA_TOPIC_RAW_LOGS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    log.info("Processor started — consuming from %s", KAFKA_TOPIC_RAW_LOGS)

    for msg in consumer:
        try:
            entry = msg.value

            # 1. Index to Elasticsearch
            index_log(es, entry)

            # 2. Run incident detection
            incident = detect(entry, r)
            if incident:
                producer.send(KAFKA_TOPIC_INCIDENTS, value=incident)
                log.info("[INCIDENT PUBLISHED] %s — %s", incident["type"], incident["service"])

        except Exception as e:
            log.error("Error processing log: %s", e, exc_info=True)


if __name__ == "__main__":
    main()
