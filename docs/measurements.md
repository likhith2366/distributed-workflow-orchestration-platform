# Measurement report — October 20, 2026

Observed local results, not design targets and not AWS results.

## Environment

Windows 11 workstation, Java 25.0.2 (code compiled for Java 21), 8 JVM-visible processors, 512 MiB max heap, local NTFS disk. Not an isolated benchmark host: an IDE, Docker Desktop and other development work were running.

## Engine throughput and scaling

Superseded by **[the scaling study](scaling.md)**: 1 → 256 workers in two regimes, before/after interleaved, JDK 21, per-run disk probes. The earlier single-point comparison in this section (653 → 1,041 wf/s at 8 workers) was measured on JDK 25.0.2, whose virtual-thread timer bug (JDK-8370887) can stall threads for seconds. Its raw files are kept in `benchmarks/superseded-jdk25.0.2/` but no longer cited.

## Process-crash recovery

`ChaosHarness 200 20261020` on JDK 21 with the patch-record WAL (`benchmarks/chaos.json`): 200 child JVMs killed at seeded acknowledged boundaries (55 after start, 52 after claim, 49 after effect, 44 after completion). 200/200 executions completed after reopening their journals; 200 downstream effects, **0 duplicates**. Recovery p50 140 ms / p99 152 ms with a 150 ms test lease (the service uses 3 s). No network partition or power-loss testing.

## DynamoDB backend on LocalStack (functional only)

`scripts/load_test.py` through nginx → 2 API replicas → LocalStack 4.4, 2 worker replicas × 8 (`benchmarks/http-localstack-400.json`): 400/400 workflows succeeded, 0 failures, exact execution/status/effect counters. Throughput was 3.7 wf/s with p99 15 s.

That number is the emulator. A direct probe of LocalStack `TransactWriteItems` measured 12 ms p50 for one caller and 158 ms p50 per call at 16 concurrent callers, so transactions are serialized at roughly 100/s. Some leases expired because emulator calls stalled beyond 3 s; those tasks were reclaimed and completed correctly. These results show behavior under stalls, not capacity. Measure on AWS before stating DynamoDB-backend throughput.

## Caveats

Percentiles are nearest-rank over recorded samples. Workflow latency includes submission and completion detection. Dispatch latency is claim time minus persisted ready time. Closed-loop harnesses cannot establish an open-loop saturation SLO, and the reported throughput is not a maximum capacity.
