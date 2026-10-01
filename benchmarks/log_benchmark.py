#!/usr/bin/env python3
"""Reproducible load test for the log analytics platform.

Measures both HTTP ingestion and eventual Elasticsearch indexing. Uses only the
Python standard library so it can run on a clean machine with Docker + Python.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import json
import math
import os
import platform
import random
import statistics
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path


def percentile(values, p):
    if not values:
        return None
    xs = sorted(values)
    if len(xs) == 1:
        return xs[0]
    k = (len(xs)-1)*p/100
    f, c = math.floor(k), math.ceil(k)
    if f == c: return xs[int(k)]
    return xs[f]*(c-k)+xs[c]*(k-f)


def http_json(method, url, payload=None, timeout=30):
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Accept": "application/json"}
    if payload is not None: headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            b = r.read()
            return r.status, (json.loads(b) if b else {}), (time.perf_counter()-t0)*1000, None
    except urllib.error.HTTPError as e:
        return e.code, None, (time.perf_counter()-t0)*1000, e.read().decode(errors='replace')[:500]
    except Exception as e:
        return 0, None, (time.perf_counter()-t0)*1000, repr(e)


def make_event(service, i):
    level = "ERROR" if i % 20 == 0 else ("WARN" if i % 7 == 0 else "INFO")
    return {
        "service": service,
        "level": level,
        "message": f"benchmark event {i} run={service}",
        "host": f"bench-host-{i%4}",
        "trace_id": f"bench-{uuid.uuid4().hex}",
        "response_time_ms": 6000 if level == "ERROR" and i % 40 == 0 else random.randint(5,900),
        "status_code": 500 if level == "ERROR" else 200,
        "environment": "benchmark",
        "metadata": {"benchmark": True, "sequence": i},
    }


def send_batch(base_url, batch):
    return http_json("POST", f"{base_url}/api/v1/logs/batch", batch, timeout=120)


def es_count(es_url, service):
    query = {"query": {"term": {"service": service}}}
    status, obj, _, err = http_json("POST", f"{es_url}/logs/_count", query, timeout=10)
    if status != 200 or not isinstance(obj, dict):
        return None, err or f"HTTP {status}"
    return int(obj.get("count", 0)), None


def healthcheck(base_url):
    status, obj, ms, err = http_json("GET", f"{base_url}/health", timeout=5)
    if status != 200:
        raise SystemExit(f"Ingestion health check failed: HTTP {status}, {err or obj}")
    return ms


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8081")
    ap.add_argument("--es-url", default="http://localhost:9200")
    ap.add_argument("--events", type=int, default=100000)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--index-timeout", type=float, default=180.0)
    ap.add_argument("--output-dir", default="benchmarks/results")
    args = ap.parse_args()
    if args.events < 1 or args.batch_size < 1 or args.batch_size > 1000 or args.concurrency < 1:
        ap.error("events/concurrency must be >=1 and batch-size must be 1..1000")

    health_ms = healthcheck(args.url)
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    service = f"benchmark-{run_id.lower()}"
    events = [make_event(service, i) for i in range(args.events)]
    batches = [events[i:i+args.batch_size] for i in range(0, len(events), args.batch_size)]
    print(f"Health OK ({health_ms:.1f} ms). Sending {args.events} events as {len(batches)} batches; concurrency={args.concurrency}")

    t0 = time.perf_counter()
    results=[]
    with cf.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futs=[ex.submit(send_batch,args.url,b) for b in batches]
        for i,f in enumerate(cf.as_completed(futs),1):
            results.append((len(batches[i-1]) if False else 0, f.result()))
            if i % max(1,len(futs)//10)==0: print(f"  batches complete: {i}/{len(futs)}")
    ingest_elapsed=time.perf_counter()-t0

    # Rebuild success counts in original batch order independently of as_completed order.
    successful_batches=0; failed_batches=0; accepted_events=0; lat=[]; errors=[]
    # We only need result status; batch sizes are uniform except the last. Re-run mapping from response count.
    for _, (status,obj,ms,err) in results:
        lat.append(ms)
        if status==202 and isinstance(obj,dict):
            successful_batches += 1
            accepted_events += int(obj.get("count",0))
        else:
            failed_batches += 1
            if err: errors.append(err)

    first_index_check=time.perf_counter()
    indexed=0; es_error=None
    deadline=time.monotonic()+args.index_timeout
    while time.monotonic()<deadline:
        c,e=es_count(args.es_url,service)
        if c is not None:
            indexed=c
            es_error=None
            if indexed>=accepted_events: break
        else:
            es_error=e
        time.sleep(0.5)
    end_to_end_elapsed=time.perf_counter()-t0

    summary={
      "run_id":run_id,
      "benchmark_service":service,
      "environment":{"platform":platform.platform(),"python":platform.python_version(),"cpu_count":os.cpu_count()},
      "config":vars(args),
      "ingestion":{
        "events_requested":args.events,
        "events_accepted":accepted_events,
        "successful_batches":successful_batches,
        "failed_batches":failed_batches,
        "elapsed_s":round(ingest_elapsed,4),
        "accepted_events_per_s":round(accepted_events/ingest_elapsed,3) if ingest_elapsed else None,
        "batch_p50_ms":round(percentile(lat,50),3) if lat else None,
        "batch_p95_ms":round(percentile(lat,95),3) if lat else None,
        "batch_p99_ms":round(percentile(lat,99),3) if lat else None,
        "batch_mean_ms":round(statistics.mean(lat),3) if lat else None,
      },
      "elasticsearch":{
        "indexed_events":indexed,
        "index_completion_rate_pct":round(100*indexed/accepted_events,3) if accepted_events else None,
        "end_to_end_elapsed_s":round(end_to_end_elapsed,4),
        "end_to_end_indexed_events_per_s":round(indexed/end_to_end_elapsed,3) if end_to_end_elapsed else None,
        "last_error":es_error,
      },
      "errors_sample":errors[:10],
    }
    out=Path(args.output_dir); out.mkdir(parents=True,exist_ok=True)
    f=out/f"log_benchmark_{run_id}.json"; f.write_text(json.dumps(summary,indent=2))
    print("\n=== RESULT ===")
    print(json.dumps(summary,indent=2))
    print(f"\nSaved: {f}")

if __name__=='__main__': main()
