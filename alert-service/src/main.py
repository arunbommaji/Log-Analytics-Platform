import json
import logging
import time
from kafka import KafkaConsumer
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_INCIDENTS, KAFKA_CONSUMER_GROUP
from src.db import ensure_schema, save_incident

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SEVERITY_EMOJI = {
    "CRITICAL": "🔴",
    "HIGH":     "🟠",
    "MEDIUM":   "🟡",
    "LOW":      "🟢",
}

def send_alert(incident: dict):
    """Simulate sending alert to Slack / PagerDuty / Email."""
    emoji = SEVERITY_EMOJI.get(incident["severity"], "⚪")
    log.info(
        "%s ALERT [%s] %s — %s | %s",
        emoji, incident["severity"], incident["type"],
        incident["service"], incident["message"][:120]
    )
    # TODO: integrate real notification channels (Slack webhook, PagerDuty, SNS)

def wait_for_db(retries: int = 15, delay: int = 4):
    import psycopg2
    for i in range(retries):
        try:
            ensure_schema()
            return
        except Exception as e:
            log.info("Waiting for DB (%d/%d): %s", i+1, retries, e)
            time.sleep(delay)
    raise RuntimeError("Database not reachable")

def main():
    wait_for_db()

    consumer = KafkaConsumer(
        KAFKA_TOPIC_INCIDENTS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    log.info("Alert manager started — consuming from %s", KAFKA_TOPIC_INCIDENTS)

    for msg in consumer:
        try:
            incident = msg.value
            save_incident(incident)
            send_alert(incident)
        except Exception as e:
            log.error("Error processing incident: %s", e, exc_info=True)

if __name__ == "__main__":
    main()
