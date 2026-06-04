import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from src.config import DATABASE_URL

log = logging.getLogger(__name__)

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS incidents (
    incident_id   VARCHAR(36) PRIMARY KEY,
    service       VARCHAR(100) NOT NULL,
    type          VARCHAR(100) NOT NULL,
    severity      VARCHAR(20) NOT NULL,
    message       TEXT NOT NULL,
    status        VARCHAR(20) DEFAULT 'OPEN',
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    resolved_at   TIMESTAMPTZ
);
"""

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def ensure_schema():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE)
        conn.commit()
    log.info("Database schema ready")

def save_incident(incident: dict):
    sql = """
        INSERT INTO incidents (incident_id, service, type, severity, message, status, created_at)
        VALUES (%(incident_id)s, %(service)s, %(type)s, %(severity)s, %(message)s, %(status)s, %(timestamp)s)
        ON CONFLICT (incident_id) DO NOTHING
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, incident)
        conn.commit()
