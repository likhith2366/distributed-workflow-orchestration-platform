"""Real HTTP + separate worker-process integration test. Python 3 standard library only.

    python scripts/integration_test.py                       # journal store, remote HTTP workers
    python scripts/integration_test.py --store dynamodb      # DynamoDB/SQS/S3/EventBridge via LocalStack

The DynamoDB mode expects an AWS-compatible endpoint (default http://127.0.0.1:4566, e.g.
`docker compose -f compose.localstack.yaml up -d localstack`) and the AWS CLI for the EventBridge tap.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
JAR = ROOT / "target" / "conductor.jar"
JAVA = str(Path(os.environ["JAVA_HOME"]) / "bin" / ("java.exe" if os.name == "nt" else "java")) if "JAVA_HOME" in os.environ else "java"
TOKEN = "integration-only-token-32-characters"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class EventTap:
    """An EventBridge rule that copies Conductor events to an SQS queue we can read."""

    def __init__(self, endpoint, bus, env):
        self.endpoint, self.bus, self.env = endpoint, bus, env
        self.queue = "events-tap-" + uuid.uuid4().hex[:8]

    def aws(self, *args):
        out = subprocess.run(["aws", "--endpoint-url", self.endpoint, "--output", "json", *args],
                             env=self.env, check=True, capture_output=True, text=True).stdout
        return json.loads(out) if out.strip() else {}

    def install(self):
        self.url = self.aws("sqs", "create-queue", "--queue-name", self.queue)["QueueUrl"]
        arn = self.aws("sqs", "get-queue-attributes", "--queue-url", self.url, "--attribute-names", "QueueArn")["Attributes"]["QueueArn"]
        self.aws("events", "put-rule", "--name", self.queue, "--event-bus-name", self.bus,
                 "--event-pattern", json.dumps({"source": ["dev.conductor"]}))
        self.aws("events", "put-targets", "--rule", self.queue, "--event-bus-name", self.bus,
                 "--targets", json.dumps([{"Id": "tap", "Arn": arn}]))

    def statuses(self, execution, timeout=20):
        seen, deadline = [], time.monotonic() + timeout
        while time.monotonic() < deadline:
            for m in self.aws("sqs", "receive-message", "--queue-url", self.url, "--max-number-of-messages", "10",
                              "--wait-time-seconds", "1").get("Messages", []):
                detail = json.loads(m["Body"])["detail"]
                if detail["executionId"] == execution:
                    seen.append(detail["status"])
            if seen and seen[-1] in ("SUCCEEDED", "FAILED", "COMPENSATED", "CANCELLED"):
                return seen
        return seen


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", choices=["journal", "dynamodb"], default="journal")
    parser.add_argument("--aws-endpoint", default=os.environ.get("CONDUCTOR_AWS_ENDPOINT", "http://127.0.0.1:4566"))
    args = parser.parse_args()
    if not JAR.exists():
        raise SystemExit("Build first: mvn package (target/conductor.jar)")
    dynamo = args.store == "dynamodb"
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    data_path = ROOT / "tmp" / ("http-" + uuid.uuid4().hex)
    data_path.mkdir(parents=True)
    suffix = uuid.uuid4().hex[:8]
    env = dict(os.environ, PORT=str(port), DATA_DIR=str(data_path), API_TOKEN=TOKEN, WORKERS="0",
               BIND_ADDRESS="127.0.0.1", CONDUCTOR_STORE=args.store, CONDUCTOR_ROLE="coordinator")
    if dynamo:
        env.update(CONDUCTOR_AWS_ENDPOINT=args.aws_endpoint, CONDUCTOR_CREATE_RESOURCES="true",
                   AWS_REGION="us-east-1", AWS_ACCESS_KEY_ID="test", AWS_SECRET_ACCESS_KEY="test",
                   CONDUCTOR_TABLE="it-" + suffix, CONDUCTOR_QUEUE="it-tasks-" + suffix,
                   CONDUCTOR_BUCKET="it-payloads-" + suffix, CONDUCTOR_EVENT_BUS="it-bus-" + suffix)
    log = open(data_path / "server.log", "w")
    server = subprocess.Popen([JAVA, "-jar", str(JAR)], cwd=ROOT, env=env, stdout=log, stderr=log)
    worker_env = dict(env, WORKERS="4", COORDINATOR_URL=base, CONDUCTOR_ROLE="worker")
    worker_log = open(data_path / "worker.log", "w")
    start_worker = lambda: subprocess.Popen([JAVA, "-jar", str(JAR)], cwd=ROOT, env=worker_env, stdout=worker_log, stderr=worker_log)
    worker = None

    def call(path, body=None, auth=True, headers=None):
        request = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json", **({"Authorization": "Bearer " + TOKEN} if auth else {}), **(headers or {})})
        with urllib.request.urlopen(request, timeout=30) as response:
            content = response.read().decode()
            return json.loads(content) if response.headers["Content-Type"].startswith("application/json") else content

    def expect_status(code, *a, **kw):
        try:
            call(*a, **kw)
        except urllib.error.HTTPError as error:
            assert error.code == code, (error.code, code)
        else:
            raise AssertionError(f"Expected HTTP {code}")

    def wait(predicate, what, timeout=30):
        deadline, last = time.monotonic() + timeout, None
        while time.monotonic() < deadline:
            last = predicate()
            if last:
                return last
            time.sleep(.05)
        raise AssertionError("Timed out waiting for " + what)

    def wait_run(execution, predicate, timeout=30):
        return wait(lambda: (lambda r: r if predicate(r) else None)(call("/api/executions/" + execution)),
                    f"execution {execution}", timeout)

    try:
        wait(lambda: _healthy(call), "coordinator health", 90)
        assert '<div id="root">' in call("/"), "React console served"
        expect_status(401, "/api/metrics", auth=False)
        expect_status(403, "/api/executions", {}, headers={"Origin": "https://untrusted.example"})
        expect_status(400, "/api/executions", {"name": "bad", "version": "1", "steps": []}, headers={"Idempotency-Key": "bad"})
        expect_status(404, "/api/executions/missing")
        tap = None
        if dynamo:
            tap = EventTap(args.aws_endpoint, env["CONDUCTOR_EVENT_BUS"], env)
            tap.install()
        definition = json.loads((ROOT / "examples/order-fulfillment.json").read_text())
        headers = {"Idempotency-Key": "http-retail"}
        execution = call("/api/executions", definition, headers=headers)["id"]
        assert call("/api/executions", definition, headers=headers)["id"] == execution
        worker = start_worker()
        wait_run(execution, lambda r: r["tasks"]["fraudReview"]["status"] == "WAITING", 90)
        call(f"/api/executions/{execution}/signal", {"step": "fraudReview"})
        run = wait_run(execution, lambda r: r["status"] == "SUCCEEDED")
        assert run["tasks"]["authorizePayment"]["attempt"] == 2
        saga = json.loads((ROOT / "examples/compensation.json").read_text())
        saga_id = call("/api/executions", saga, headers={"Idempotency-Key": "http-saga"})["id"]
        saga_run = wait_run(saga_id, lambda r: r["status"] == "COMPENSATED")
        assert saga_run["tasks"]["$undo:charge"]["completedSequence"] < saga_run["tasks"]["$undo:reserve"]["completedSequence"]
        # A long activity exceeds one lease; heartbeats must keep its attempt at one.
        long_run = {"name": "heartbeat", "version": "1", "steps": [{"id": "long", "action": "sleep", "durationMs": 4500}]}
        long_id = call("/api/executions", long_run, headers={"Idempotency-Key": "heartbeat"})["id"]
        result = wait_run(long_id, lambda r: r["status"] == "SUCCEEDED")
        assert result["tasks"]["long"]["attempt"] == 1, result["tasks"]["long"]
        # Kill a separately running worker while it owns a task, then replace it.
        crash_id = call("/api/executions", long_run, headers={"Idempotency-Key": "worker-crash"})["id"]
        wait_run(crash_id, lambda r: r["tasks"]["long"]["status"] == "RUNNING")
        worker.kill()
        worker.wait(timeout=10)
        worker = start_worker()
        result = wait_run(crash_id, lambda r: r["status"] == "SUCCEEDED", 60)
        assert result["tasks"]["long"]["attempt"] == 2, result["tasks"]["long"]
        assert result["tasks"]["long"]["fence"] == 2
        waiting = {"name": "cancel", "version": "1", "steps": [{"id": "approval", "action": "approval"}]}
        cancel_id = call("/api/executions", waiting, headers={"Idempotency-Key": "cancel"})["id"]
        assert call(f"/api/executions/{cancel_id}/cancel", {})["status"] == "CANCELLED"
        checks = "authentication, origin checks, validation, idempotency, retail DAG, approvals, saga, heartbeats, worker kill/reclaim, cancellation"
        if dynamo:
            # Results above the 32 KiB threshold are stored in S3 and inlined on read.
            big = {"name": "large", "version": "1", "steps": [{"id": "report", "value": {"blob": "x" * 40_000}}]}
            big_id = call("/api/executions", big, headers={"Idempotency-Key": "large"})["id"]
            big_run = wait_run(big_id, lambda r: r["status"] == "SUCCEEDED")
            assert big_run["tasks"]["report"].get("resultStoredIn") == "s3"
            assert len(big_run["tasks"]["report"]["result"]["blob"]) == 40_000
            # Durable timer longer than nothing, delivered by SQS delay rather than a sleeping thread.
            timer = {"name": "timer", "version": "1", "steps": [{"id": "wait", "action": "wait", "delayMs": 2000}]}
            timer_id = call("/api/executions", timer, headers={"Idempotency-Key": "timer"})["id"]
            timer_run = wait_run(timer_id, lambda r: r["status"] == "SUCCEEDED")
            t = timer_run["tasks"]["wait"]
            assert t["claimedAt"] >= t["readyAt"], t
            statuses = tap.statuses(execution)
            assert statuses and statuses[0] == "RUNNING" and statuses[-1] == "SUCCEEDED", statuses
            checks += ", S3 payload offload, SQS-delayed timer, EventBridge lifecycle events"
        wait(lambda: call("/api/metrics")["engine"]["active"] == 0, "no active executions", 15)
        wait(lambda: call("/api/metrics")["ledgerEffects"] == 6, "six ledger effects", 15)
        print(f"PASS HTTP integration ({args.store}): {checks}")
    finally:
        if worker is not None:
            worker.kill()
            worker.wait(timeout=10)
        server.terminate()
        try:
            server.wait(timeout=20)
        except subprocess.TimeoutExpired:
            server.kill()
        log.close()
        worker_log.close()


def _healthy(call):
    try:
        return call("/healthz", auth=False)["status"] == "ok"
    except (OSError, urllib.error.URLError, ValueError):
        return False


if __name__ == "__main__":
    main()
