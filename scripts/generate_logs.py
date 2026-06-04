#!/usr/bin/env python3
"""
Simulates 8 services sending realistic logs to the ingestion API.
Run after docker compose up:  python3 scripts/generate_logs.py
"""
import requests
import random
import time
import sys
from datetime import datetime

API = "http://localhost:8081/api/v1/logs"

SERVICES = [
    "order-service", "payment-service", "inventory-service",
    "auth-service", "api-gateway", "notification-service",
    "user-service", "reporting-service"
]
HOSTS = ["server-01", "server-02", "server-03", "server-04"]
ENVS  = ["production", "staging"]

MESSAGES = {
    "INFO": [
        "Request processed successfully",
        "Cache hit for key user:{}",
        "Order {} confirmed",
        "Payment processed: ${}",
        "Inventory reserved for order {}",
        "User {} logged in",
        "Health check passed",
        "Message published to Kafka topic",
    ],
    "WARN": [
        "Response time elevated: {}ms",
        "Cache miss — fetching from DB",
        "Retry attempt {} for request",
        "Rate limit approaching for client {}",
        "Connection pool at 80% capacity",
        "Slow query detected: {}ms",
    ],
    "ERROR": [
        "Failed to process payment: {}",
        "Database connection failed after {} retries",
        "NullPointerException in OrderService.confirm()",
        "timeout exceeded for downstream call",
        "connection refused to payment gateway",
        "Circuit breaker open for {}",
        "Failed to reserve inventory: out of stock",
        "authentication failed for user {}",
    ],
    "DEBUG": [
        "Processing message from Kafka offset {}",
        "SQL query executed in {}ms",
        "Deserializing payload: {} bytes",
        "Cache set with TTL {}s",
    ]
}

WEIGHTS = {"DEBUG": 5, "INFO": 60, "WARN": 20, "ERROR": 15}
LEVELS  = list(WEIGHTS.keys())

def random_message(level: str) -> tuple[str, dict]:
    template = random.choice(MESSAGES[level])
    msg = template.format(
        random.randint(100, 9999),
        random.randint(1, 50),
        random.choice(["card_001", "tok_visa", "pm_123"]),
    )
    metadata = {}
    rt = None
    if level == "WARN":
        rt = random.randint(1000, 4999)
    elif level == "ERROR":
        rt = random.randint(3000, 12000)
    else:
        rt = random.randint(10, 800)
    return msg, rt

def send_log(service: str, level: str, burst: bool = False):
    msg, rt = random_message(level)
    payload = {
        "service":          service,
        "level":            level,
        "message":          msg,
        "host":             random.choice(HOSTS),
        "trace_id":         f"trace-{random.randint(10000,99999)}",
        "response_time_ms": rt,
        "status_code":      500 if level == "ERROR" else random.choice([200,200,200,201,204,304]),
        "environment":      random.choice(ENVS),
    }
    try:
        r = requests.post(API, json=payload, timeout=3)
        status = "✓" if r.status_code == 202 else f"✗{r.status_code}"
        print(f"  {status} [{level:8s}] {service}: {msg[:60]}")
    except Exception as e:
        print(f"  ✗ {e}")

def main():
    total    = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    interval = float(sys.argv[2]) if len(sys.argv) > 2 else 0.2

    print(f"Sending {total} logs to {API} (interval={interval}s)")
    print("Press Ctrl+C to stop\n")

    for i in range(total):
        service = random.choice(SERVICES)
        level   = random.choices(LEVELS, weights=[WEIGHTS[l] for l in LEVELS])[0]

        # Every 50 logs, simulate an error spike on one service
        if i % 50 == 0 and i > 0:
            burst_svc = random.choice(SERVICES)
            print(f"\n⚡ ERROR BURST on {burst_svc}")
            for _ in range(7):
                send_log(burst_svc, "ERROR")
                time.sleep(0.05)

        send_log(service, level)
        time.sleep(interval)

    print(f"\n✅ Sent {total} logs")

if __name__ == "__main__":
    main()
