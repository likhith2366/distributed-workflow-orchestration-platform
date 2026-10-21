"""End-to-end smoke test against an already running deployment (compose, LocalStack stack, or AWS).

    API_TOKEN=... python scripts/smoke_test.py http://127.0.0.1:8080

Runs the retail workflow (retry + approval + parallel join + timer), the compensation saga,
and a long activity that must survive lease renewal. Python 3 standard library only.
"""
import json
import os
from pathlib import Path
import sys
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]


def main():
    base = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8080").rstrip("/")
    token = os.environ.get("API_TOKEN", "")

    def call(path, body=None, headers=None):
        request = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "Authorization": "Bearer " + token, **(headers or {})})
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode())

    def start(definition):
        return call("/api/executions", definition, {"Idempotency-Key": "smoke-" + uuid.uuid4().hex})["id"]

    def wait(execution, predicate, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            run = call("/api/executions/" + execution)
            if predicate(run):
                return run
            time.sleep(0.1)
        raise AssertionError(f"Timed out: {json.dumps(run)[:2000]}")

    deadline = time.monotonic() + 120
    while True:
        try:
            if call("/healthz")["status"] == "ok":
                break
        except OSError:
            pass
        if time.monotonic() > deadline:
            raise SystemExit("Deployment never became healthy")
        time.sleep(1)
    began = time.monotonic()
    retail = start(json.loads((ROOT / "examples/order-fulfillment.json").read_text()))
    saga = start(json.loads((ROOT / "examples/compensation.json").read_text()))
    long = start({"name": "lease", "version": "1", "steps": [{"id": "long", "action": "sleep", "durationMs": 5000}]})
    wait(retail, lambda r: r["tasks"]["fraudReview"]["status"] == "WAITING")
    call(f"/api/executions/{retail}/signal", {"step": "fraudReview"})
    r = wait(retail, lambda r: r["status"] == "SUCCEEDED")
    assert r["tasks"]["authorizePayment"]["attempt"] == 2, r["tasks"]["authorizePayment"]
    wait(saga, lambda r: r["status"] == "COMPENSATED")
    l = wait(long, lambda r: r["status"] == "SUCCEEDED")
    assert l["tasks"]["long"]["attempt"] == 1, l["tasks"]["long"]
    metrics = call("/api/metrics")
    print(f"PASS smoke ({metrics['engine']['backend']}) in {time.monotonic() - began:.1f}s: "
          "retry, approval, parallel join, timer, compensation, lease renewal")


if __name__ == "__main__":
    main()
