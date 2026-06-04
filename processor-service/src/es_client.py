import logging
from elasticsearch import Elasticsearch
from src.config import ELASTICSEARCH_HOST, ELASTICSEARCH_INDEX

log = logging.getLogger(__name__)

INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "log_id":          {"type": "keyword"},
            "timestamp":       {"type": "date"},
            "service":         {"type": "keyword"},
            "level":           {"type": "keyword"},
            "message":         {"type": "text", "analyzer": "standard"},
            "host":            {"type": "keyword"},
            "trace_id":        {"type": "keyword"},
            "response_time_ms":{"type": "integer"},
            "status_code":     {"type": "integer"},
            "environment":     {"type": "keyword"},
            "incident_id":     {"type": "keyword"},
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "1s",
    }
}

def create_es_client() -> Elasticsearch:
    return Elasticsearch(ELASTICSEARCH_HOST, request_timeout=10)

def ensure_index(es: Elasticsearch) -> None:
    if not es.indices.exists(index=ELASTICSEARCH_INDEX):
        es.indices.create(index=ELASTICSEARCH_INDEX, body=INDEX_MAPPING)
        log.info("Created Elasticsearch index: %s", ELASTICSEARCH_INDEX)

def index_log(es: Elasticsearch, doc: dict) -> None:
    es.index(index=ELASTICSEARCH_INDEX, id=doc["log_id"], document=doc)
