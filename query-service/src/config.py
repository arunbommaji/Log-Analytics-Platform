import os
ELASTICSEARCH_HOST  = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "logs")
DATABASE_URL        = os.getenv("DATABASE_URL", "postgresql://incidentuser:incidentpass@localhost:5432/incidentsdb")
