import logging, time
from contextlib import asynccontextmanager
from typing import List
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from src.models import LogEntry, LogEntryEnriched
from src.kafka_producer import create_producer, publish_log
from src.config import KAFKA_TOPIC_RAW_LOGS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
producer = None

def connect_kafka(retries=20, delay=5):
    for i in range(retries):
        try:
            p = create_producer()
            log.info("Kafka connected")
            return p
        except Exception as e:
            log.info("Waiting for Kafka (%d/%d): %s", i+1, retries, e)
            time.sleep(delay)
    raise RuntimeError("Kafka unavailable")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = connect_kafka()
    yield
    if producer:
        producer.flush()
        producer.close()

app = FastAPI(title="Log Ingestion API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.post("/api/v1/logs", status_code=202)
def ingest_log(entry: LogEntry):
    enriched = LogEntryEnriched(**entry.model_dump())
    publish_log(producer, KAFKA_TOPIC_RAW_LOGS, enriched.model_dump())
    return {"log_id": enriched.log_id, "status": "accepted"}

@app.post("/api/v1/logs/batch", status_code=202)
def ingest_batch(entries: List[LogEntry]):
    if len(entries) > 1000:
        raise HTTPException(400, "Batch too large")
    log_ids = []
    for entry in entries:
        enriched = LogEntryEnriched(**entry.model_dump())
        publish_log(producer, KAFKA_TOPIC_RAW_LOGS, enriched.model_dump())
        log_ids.append(enriched.log_id)
    return {"count": len(log_ids), "log_ids": log_ids, "status": "accepted"}
