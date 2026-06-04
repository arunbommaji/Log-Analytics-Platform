import os

KAFKA_BOOTSTRAP_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RAW_LOGS     = os.getenv("KAFKA_TOPIC_RAW_LOGS", "raw-logs")
KAFKA_TOPIC_INCIDENTS    = os.getenv("KAFKA_TOPIC_INCIDENTS", "incidents")
KAFKA_CONSUMER_GROUP     = os.getenv("KAFKA_CONSUMER_GROUP", "processor-group")
ELASTICSEARCH_HOST       = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
ELASTICSEARCH_INDEX      = os.getenv("ELASTICSEARCH_INDEX", "logs")
REDIS_HOST               = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT               = int(os.getenv("REDIS_PORT", "6379"))
