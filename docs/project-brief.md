# Distributed Workflow Orchestration Platform

## Project purpose

Build a portfolio system that demonstrates durable asynchronous service design through a retail fulfillment workflow. Its relevance to Amazon is the engineering problem: coordinating inventory, payment, fraud review, fulfillment, and notification despite retries and worker loss. The implementation is independent and does not reproduce Amazon internal systems.

## Implemented

Spring Boot 4 service (coordinator and worker roles in one jar) and a React + TypeScript console. One pure workflow state machine with two durable stores:

- a group-committed, checksummed write-ahead log, for a single node;
- DynamoDB per-execution transactions with a transactional outbox to SQS, S3 offload for large results, and EventBridge lifecycle events, for horizontal scale.

Fenced leases, parallel DAGs, exponential retries, durable timers, human approval, pinned definitions, reverse-order compensation, and an idempotent example ledger. TypeScript CDK provisions the managed-service architecture on ECS Fargate.

Not yet done: an AWS deployment, AWS measurements, TLS at the load balancer, tenant isolation, and tracing.

## Corrected guarantees

- **Recovery:** replay acknowledged state transitions after process failure. No power-loss or multi-AZ durability claim.
- **Execution:** at-least-once activity attempts, with stale completion rejection.
- **Effects:** one effect per key only when the downstream system atomically enforces idempotency. The included ledger does this.
- **Compensation:** retry declared undo activities in reverse causal order; surface exhausted compensation for human action.
- **Versioning:** pin the full workflow definition when an execution starts.
- **Timers:** durable due times and polling, with scale limits stated.

## Evidence requirements

Every performance claim must link to raw results and give environment, workload, concurrency, and durability settings. Every recovery claim must name the injected failure, number of trials, and success criterion. An observed zero in a finite test is not a universal guarantee.

The repository includes a core correctness suite, real HTTP/worker-process integration, seeded child-JVM kills at four acknowledgment boundaries, and a bounded-in-flight benchmark. It preserves both successful measurements and the Windows lock-release failure that changed the harness.

## AWS progression

1. ~~DynamoDB conditional transactions and an outbox~~ — implemented, tested on LocalStack.
2. ~~SQS dispatch with DLQ; due-timer handling; S3 payloads; EventBridge events~~ — implemented, tested on LocalStack.
3. Deploy the CDK stack; run `smoke_test.py` and `load_test.py` from inside the VPC.
4. Publish AWS measurements on fixed task sizes, including transaction conflicts, queue age, and cost per 1,000 workflows.
5. Add ALB TLS, per-tenant authorization, and trace propagation.

## Hiring narrative

Lead with a concrete failure: "The worker recorded an effect, then died before acknowledging completion." Explain the stable effect key, durable downstream result, lease expiry, higher fencing token, and recovery test. Explain the journal's single-writer trade-off and how the DynamoDB backend removes it: per-execution transactions and an outbox.

Keep Leadership Principle examples grounded in the work: the lock-release discovery for Dive Deep; rejecting blanket exactly-once claims for high standards; and an executable minimal core for simplification. Do not present generated code or planned integrations as personal production experience.
