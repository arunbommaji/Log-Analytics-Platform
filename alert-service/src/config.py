import os
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_INCIDENTS   = os.getenv("KAFKA_TOPIC_INCIDENTS", "incidents")
KAFKA_CONSUMER_GROUP    = os.getenv("KAFKA_CONSUMER_GROUP", "alert-group")
REDIS_HOST              = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT              = int(os.getenv("REDIS_PORT", "6379"))
DATABASE_URL            = os.getenv("DATABASE_URL", "postgresql://incidentuser:incidentpass@localhost:5432/incidentsdb")
