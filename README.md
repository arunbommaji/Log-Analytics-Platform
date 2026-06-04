# Real-Time Log Analytics & Incident Detection Platform

Real-time log processing pipeline that ingests logs from any service, indexes them in Elasticsearch, and automatically detects incidents using pattern matching, error rate analysis, and latency anomaly detection.

## 🚀 Quick Start

### Prerequisites
- Docker Desktop running (4GB+ RAM)

### Step 1 — Clone and start
```bash
git clone https://github.com/arunbommaji/log-analytics-platform.git
cd log-analytics-platform
docker compose up -d
```
First run takes 5–10 minutes (builds Python services, downloads Elasticsearch).

### Step 2 — Open the dashboard
**http://localhost:3000** — live log stream with incident feed

### Step 3 — Generate logs
```bash
pip install requests
python3 scripts/generate_logs.py 200 0.1
```
Sends 200 realistic logs from 8 simulated services, triggering incident detection.

### Step 4 — Send a single log via API
```bash
curl -X POST http://localhost:8081/api/v1/logs \
  -H "Content-Type: application/json" \
  -d '{"service":"payment-service","level":"ERROR","message":"NullPointerException in OrderService","response_time_ms":5500}'
```

---

## 🌐 Service URLs

| Service | URL | Description |
|---|---|---|
| **Dashboard** | http://localhost:3000 | Live log stream + incident feed |
| **Ingestion API** | http://localhost:8081 | POST logs via REST |
| **Query API** | http://localhost:8082 | Search logs, get incidents |
| **Kafka UI** | http://localhost:8090 | View Kafka topics and messages |
| **Kibana** | http://localhost:5601 | Elasticsearch visualization |
| **Elasticsearch** | http://localhost:9200 | Direct ES access |

---

## ⚡ Incident Detection Rules

| Rule | Trigger | Severity |
|---|---|---|
| Error rate spike | 5+ errors in 60 seconds from same service | HIGH |
| Critical patterns | NullPointerException, OOM, connection refused, timeout | CRITICAL |
| High latency | Response time > 5000ms | MEDIUM |
| Auth failures | 401/403 patterns, authentication failed | CRITICAL |
| DB connection failure | Database connection failed | CRITICAL |

---

## 📡 API Reference

### Ingestion API (port 8081)
```
POST /api/v1/logs         Single log entry
POST /api/v1/logs/batch   Batch up to 1000 logs
GET  /health
```

### Query API (port 8082)
```
GET /api/v1/logs?service=payment-service&level=ERROR&q=timeout&size=50
GET /api/v1/logs/stats
GET /api/v1/incidents?status=OPEN&severity=HIGH
GET /api/v1/incidents/summary
```

---

## 🏗️ Architecture

```
Services → Ingestion API (FastAPI) → Kafka [raw-logs]
                                         ↓
                               Processor Service
                               ├── Index to Elasticsearch
                               └── Run incident detection rules
                                         ↓
                               Kafka [incidents]
                                         ↓
                               Alert Manager → PostgreSQL
                                         ↓
                               Dashboard / Query API ← Elasticsearch
```

## 🛑 Stop
```bash
docker compose down        # stop containers
docker compose down -v     # stop + wipe data
```

## 🔧 Tech Stack
Python 3.11 · FastAPI · Apache Kafka · Elasticsearch 8 · Kibana · Redis · PostgreSQL · Docker
