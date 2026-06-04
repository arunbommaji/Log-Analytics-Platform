import re
import json
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional
import redis as Redis
from src.config import REDIS_HOST, REDIS_PORT

log = logging.getLogger(__name__)

# ── Tunable thresholds ──────────────────────────────────────────────────────
ERROR_WINDOW_SECONDS  = 60
ERROR_THRESHOLD       = 5      # errors/window before incident fires
LATENCY_THRESHOLD_MS  = 5000   # ms before high-latency incident fires
DEDUP_TTL_SECONDS     = 300    # suppress duplicate incidents for 5 min

CRITICAL_PATTERNS = [
    (r"OutOfMemoryError|out of memory",   "MEMORY_EXHAUSTED"),
    (r"NullPointerException|null pointer","NULL_POINTER"),
    (r"connection refused|ECONNREFUSED",  "CONNECTION_REFUSED"),
    (r"database.*fail|db.*connect.*fail", "DB_CONNECTION_FAILED"),
    (r"circuit breaker.*open",            "CIRCUIT_BREAKER_OPEN"),
    (r"timeout|timed? out",               "TIMEOUT"),
    (r"disk.*full|no space left",         "DISK_FULL"),
    (r"authentication.*fail|401|403",     "AUTH_FAILURE"),
]


def create_redis() -> Redis.Redis:
    return Redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def _make_incident(service: str, itype: str, severity: str, message: str) -> dict:
    return {
        "incident_id":  str(uuid.uuid4()),
        "service":      service,
        "type":         itype,
        "severity":     severity,    # LOW | MEDIUM | HIGH | CRITICAL
        "message":      message,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "status":       "OPEN",
    }


def _is_duplicate(r: Redis.Redis, service: str, itype: str) -> bool:
    key = f"incident:dedup:{service}:{itype}"
    created = r.set(key, "1", nx=True, ex=DEDUP_TTL_SECONDS)
    return not created   # True = duplicate (key already existed)


def detect(log_entry: dict, r: Redis.Redis) -> Optional[dict]:
    service = log_entry.get("service", "unknown")
    level   = log_entry.get("level", "INFO").upper()
    message = log_entry.get("message", "")
    rt_ms   = log_entry.get("response_time_ms")

    # ── Rule 1: critical pattern matching ───────────────────────────────────
    for pattern, itype in CRITICAL_PATTERNS:
        if re.search(pattern, message, re.IGNORECASE):
            if not _is_duplicate(r, service, itype):
                log.warning("[INCIDENT] Pattern %s in %s", itype, service)
                return _make_incident(service, itype, "CRITICAL",
                    f"Critical pattern '{itype}' detected in {service}: {message[:200]}")

    # ── Rule 2: error rate spike ────────────────────────────────────────────
    if level in ("ERROR", "CRITICAL"):
        count_key = f"error_rate:{service}"
        count = r.incr(count_key)
        if count == 1:
            r.expire(count_key, ERROR_WINDOW_SECONDS)
        if count == ERROR_THRESHOLD:
            r.delete(count_key)
            itype = "ERROR_RATE_SPIKE"
            if not _is_duplicate(r, service, itype):
                log.warning("[INCIDENT] Error spike in %s (%d in %ds)", service, count, ERROR_WINDOW_SECONDS)
                return _make_incident(service, itype, "HIGH",
                    f"Error rate spike: {count} errors in {ERROR_WINDOW_SECONDS}s from {service}")

    # ── Rule 3: high latency ────────────────────────────────────────────────
    if rt_ms and rt_ms > LATENCY_THRESHOLD_MS:
        itype = "HIGH_LATENCY"
        if not _is_duplicate(r, service, itype):
            log.warning("[INCIDENT] High latency %dms in %s", rt_ms, service)
            return _make_incident(service, itype, "MEDIUM",
                f"High latency detected: {rt_ms}ms in {service}")

    return None
