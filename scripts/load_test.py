"""Closed-loop HTTP load test: every client submits a workflow, polls until it finishes, repeats.

    API_TOKEN=... python scripts/load_test.py http://host:8080 --workflows 500 --clients 16 --out benchmarks/x.json

Measures through the public API, so it includes HTTP, auth, serialization, the backend, and
dispatch. Python 3 standard library only; the client itself can become the bottleneck at high
rates, so record client CPU alongside results.
"""
import argparse
import json
import os
import platform
import threading
import time
import urllib.request
import uuid
from datetime import datetime, timezone

DEFINITION = {"name": "load", "version": "1", "steps": [
    {"id": "reserve", "action": "ledger", "maxAttempts": 20},
    {"id": "charge", "action": "ledger", "maxAttempts": 20, "dependsOn": ["reserve"]},
    {"id": "notify", "action": "echo", "maxAttempts": 20, "dependsOn": ["charge"]}]}
TERMINAL = {"SUCCEEDED", "FAILED", "COMPENSATED", "COMPENSATION_FAILED", "CANCELLED"}


def percentile(values, p):
    ordered = sorted(values)
    return ordered[max(0, min(len(ordered) - 1, -(-len(ordered) * p // 100) - 1))] if ordered else 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("base")
    parser.add_argument("--workflows", type=int, default=300)
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--poll-ms", type=int, default=20)
    parser.add_argument("--label", default="")
    parser.add_argument("--out")
    args = parser.parse_args()
    base, token = args.base.rstrip("/"), os.environ.get("API_TOKEN", "")
    headers = {"Content-Type": "application/json", "Authorization": "Bearer " + token}

    def call(path, body=None, extra=None):
        request = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(),
                                         headers={**headers, **(extra or {})})
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.loads(response.read())

    latencies, failures, lock = [], [], threading.Lock()
    remaining = iter(range(args.workflows))
    run_id = uuid.uuid4().hex[:8]

    def client():
        while True:
            with lock:
                i = next(remaining, None)
            if i is None:
                return
            start = time.perf_counter()
            try:
                execution = call("/api/executions", DEFINITION, {"Idempotency-Key": f"load-{run_id}-{i}"})["id"]
                while (status := call("/api/executions/" + execution)["status"]) not in TERMINAL:
                    time.sleep(args.poll_ms / 1000)
                with lock:
                    (latencies if status == "SUCCEEDED" else failures).append(
                        (time.perf_counter() - start) * 1000 if status == "SUCCEEDED" else status)
            except Exception as e:  # noqa: BLE001 - recorded, not hidden
                with lock:
                    failures.append(repr(e))

    began = time.perf_counter()
    cpu_before = time.process_time()
    threads = [threading.Thread(target=client) for _ in range(args.clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    seconds = time.perf_counter() - began
    metrics = call("/api/metrics")
    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(), "label": args.label, "target": base,
        "scope": "HTTP closed loop through the public API",
        "backend": metrics["engine"].get("backend"), "workflows": args.workflows, "clients": args.clients,
        "pollMs": args.poll_ms, "succeeded": len(latencies), "failed": len(failures), "failures": failures[:10],
        "seconds": round(seconds, 3), "workflowsPerSecond": round(len(latencies) / seconds, 2),
        "workflowP50Ms": round(percentile(latencies, 50), 1), "workflowP99Ms": round(percentile(latencies, 99), 1),
        "clientCpuSeconds": round(time.process_time() - cpu_before, 2), "clientHost": platform.platform(),
        "serverMetrics": metrics,
    }
    print(json.dumps(result, indent=2))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2)
    if failures:
        raise SystemExit(f"{len(failures)} workflows failed")


if __name__ == "__main__":
    main()
