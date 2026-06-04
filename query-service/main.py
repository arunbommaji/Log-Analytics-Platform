import logging
from typing import Optional, List
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import Elasticsearch
import psycopg2
from psycopg2.extras import RealDictCursor
from src.config import ELASTICSEARCH_HOST, ELASTICSEARCH_INDEX, DATABASE_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="Log Query API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

es  = Elasticsearch(ELASTICSEARCH_HOST, request_timeout=10)


def db_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


@app.get("/health")
def health():
    return {"status": "healthy", "service": "query-service"}


@app.get("/api/v1/logs")
def search_logs(
    service:  Optional[str]  = Query(None),
    level:    Optional[str]  = Query(None),
    q:        Optional[str]  = Query(None, description="Full-text search"),
    from_:    Optional[str]  = Query(None, alias="from"),
    to:       Optional[str]  = Query(None),
    size:     int            = Query(50, le=500),
):
    """Search logs with filters and full-text search."""
    must_clauses = []
    if service:
        must_clauses.append({"term": {"service": service}})
    if level:
        must_clauses.append({"term": {"level": level.upper()}})
    if q:
        must_clauses.append({"match": {"message": q}})
    if from_ or to:
        date_range = {}
        if from_: date_range["gte"] = from_
        if to:    date_range["lte"] = to
        must_clauses.append({"range": {"timestamp": date_range}})

    body = {
        "query": {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}},
        "sort":  [{"timestamp": {"order": "desc"}}],
        "size":  size,
    }

    try:
        res = es.search(index=ELASTICSEARCH_INDEX, body=body)
        hits = [h["_source"] for h in res["hits"]["hits"]]
        return {"total": res["hits"]["total"]["value"], "logs": hits}
    except Exception as e:
        log.error("ES search failed: %s", e)
        raise HTTPException(500, f"Search failed: {str(e)}")


@app.get("/api/v1/logs/stats")
def log_stats():
    """Aggregated stats: error rate per service, log level distribution."""
    body = {
        "size": 0,
        "aggs": {
            "by_service": {
                "terms": {"field": "service", "size": 20},
                "aggs": {
                    "by_level": {"terms": {"field": "level", "size": 10}},
                    "avg_response_time": {"avg": {"field": "response_time_ms"}},
                }
            },
            "by_level": {"terms": {"field": "level", "size": 10}},
            "logs_over_time": {
                "date_histogram": {
                    "field": "timestamp",
                    "calendar_interval": "minute",
                    "min_doc_count": 0,
                },
                "aggs": {
                    "error_count": {
                        "filter": {"terms": {"level": ["ERROR", "CRITICAL"]}}
                    }
                }
            }
        }
    }
    try:
        res = es.search(index=ELASTICSEARCH_INDEX, body=body)
        return res["aggregations"]
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/v1/incidents")
def get_incidents(
    status:   Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    service:  Optional[str] = Query(None),
    limit:    int           = Query(50, le=200),
):
    """Get incidents from PostgreSQL."""
    where = []
    params = []
    if status:
        where.append("status = %s"); params.append(status.upper())
    if severity:
        where.append("severity = %s"); params.append(severity.upper())
    if service:
        where.append("service = %s"); params.append(service)

    sql = "SELECT * FROM incidents"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)

    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        return {"total": len(rows), "incidents": [dict(r) for r in rows]}
    except Exception as e:
        log.error("DB query failed: %s", e)
        raise HTTPException(500, str(e))


@app.get("/api/v1/incidents/summary")
def incident_summary():
    """Open incident counts by severity."""
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT severity, COUNT(*) as count
                    FROM incidents WHERE status = 'OPEN'
                    GROUP BY severity
                """)
                rows = cur.fetchall()
        return {r["severity"]: r["count"] for r in rows}
    except Exception as e:
        raise HTTPException(500, str(e))
