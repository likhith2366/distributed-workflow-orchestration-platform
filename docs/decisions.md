# Decision log

## 2026-10-20 — Verify semantics before expanding cloud integrations

**Decision:** implement an executable Java 21 core with a durable local journal and independent HTTP workers before the DynamoDB/SQS adapter.

**Reason:** the repository began with a PDF, and Docker was not running. A dependency-free path allows local validation of failure semantics now.

**Rejected:** claiming that resource definitions or mocks establish an AWS distributed-systems result.

**Cost:** single coordinator, full-projection event amplification, no managed-store scaling. Spring/React remain optional later integration work.

## 2026-10-20 — Make the effect boundary explicit

**Decision:** put an idempotent effect ledger behind a separately durable journal. Keep one effect key across retries.

**Reason:** a coordinator completion record cannot prevent a payment from happening twice after an ambiguous crash. The downstream service must enforce uniqueness.

**Cost:** arbitrary activity integrations need their own idempotency contract; fenced workflow writes are not enough.

## 2026-10-20 — Drain parallel branches before compensation

**Decision:** permanent failure stops pending normal tasks, waits for running branches, then builds a reverse-completion compensation chain.

**Reason:** compensating immediately could miss a concurrently completing branch's effect.

**Cost:** failed workers delay compensation until lease recovery or exhaustion. Unacknowledged effects at exhaustion require reconciliation.

## 2026-10-20 — Store complete projections in transition records

**Decision:** a simple framed WAL stores transition type and resultant execution state.

**Reason:** an auditable atomic write boundary and deterministic recovery without implementing arbitrary-code replay.

**Cost:** write amplification and unbounded history. Delta events, snapshots, and retention need separate design and migration tests.

## 2026-10-20 — Windows process exit did not immediately free the lock

**Observation:** the first real-process chaos run failed opening a journal immediately after `destroyForcibly()` plus `waitFor()`.

**Fix:** the harness retries only the explicit "data directory already in use" condition for up to five seconds. It does not retry checksum corruption. Recovery latency includes the lock-release delay.

**Evidence:** the subsequent 200-kill seeded run recovered all executions. This is an observed local finding, not an invented interview story.

## 2026-10-20 — Keep metrics reproducible and scoped

**Decision:** raw JSON reports record workload, JVM, OS, concurrency, durability mode, seed, and test scope. Resume language identifies local measurements.

**Reason:** a large number without conditions is weak evidence. Reliability claims must name the downstream contract and injected failures.

**Cost:** numbers may look smaller than unverifiable portfolio claims; they withstand detailed questions.

## 2026-10-20 — Spring Boot 4, React + TypeScript, and a DynamoDB backend

**Decision:** move the service to Spring Boot 4.1 (one jar, `coordinator` or `worker` role), rewrite the console in React 19 + TypeScript (Vite, bundled into the jar), and add a `dynamodb` store using DynamoDB, SQS, S3 and EventBridge. Keep the journal store.

**Reason:** the state machine had been proven locally; the remaining gap was the scalable architecture and production-shaped tooling. Extracting `core/Workflow` as pure functions let both stores share identical semantics, so the existing tests constrain the new backend.

**Kept:** the strict JSON codec for request bodies (duplicate keys rejected) instead of Jackson, so the API accepts exactly what the journal persists.

**Cost:** a 43 MB jar (AWS SDK), a Node toolchain in the build, and two backends to maintain.

## 2026-10-20 — Transactional outbox instead of "write then send"

**Decision:** every DynamoDB transition writes its SQS messages and EventBridge events as an outbox item in the same transaction; the writer sends and deletes it, and a relay re-sends leftovers.

**Reason:** a commit followed by a separate `SendMessage` loses dispatch if the process dies in between. A test (`outboxRelayDeliversDispatchesAfterAFailedFastPath`) forces that gap.

**Cost:** extra writes per transition, and at-least-once dispatch. Claims are conditional, so duplicates are dropped.

## 2026-10-20 — Long-poll capped at a third of the lease

**Observation:** the first integration run after adding long-polling to remote workers reclaimed a killed worker's task as attempt **3**, not 2. The dead worker's open long-polls stayed alive on the server and one of them claimed the task when its lease expired, for a process that no longer existed.

**Fix:** cap server-side waits at `lease/3`, so a dead worker's polls end before its lease can expire.

## 2026-10-20 — Exact counters move into the transaction

**Observation:** with per-process buffered counters, killing a worker less than a second after it finished a workflow left "in progress" stuck at 1. The buffered `RUNNING → SUCCEEDED` delta died with the process.

**Fix:** execution, per-status and effect counters are `ADD`ed to a random `STATS#n` shard inside the causing transaction. Throughput counters stay buffered and are labelled best-effort.

## 2026-10-20 — Benchmark harness changed with the engine

**Observation:** after the engine optimization, the single-threaded v0.1 harness reported *half* the previous throughput while latency improved. The harness submitted serially, one flush per submission.

**Decision:** concurrent closed-loop clients; the old engine re-measured with the new harness for a like-for-like comparison. Both harness results are preserved.

## 2026-10-20 — LocalStack is functional evidence only

**Observation:** the DynamoDB backend ran at under 4 wf/s on LocalStack. A direct probe showed the emulator serializing `TransactWriteItems` (~158 ms per call at 16 concurrent callers).

**Decision:** report LocalStack runs as correctness evidence (all scenarios pass, including stalls long enough to expire leases) and make no throughput claims for the DynamoDB backend until it is measured on AWS.

## 2026-10-20 — Scaling study: what was tried, what was kept

**Question:** does durable throughput scale with workers, and if not, where and why does it flatten? (See [scaling](scaling.md) for the measurements.)

All figures: interleaved A/B on JDK 21, medians of 3 (raw files in `benchmarks/scaling/final` and `benchmarks/scaling/options-jdk21`). An earlier pass on JDK 25.0.2 was discarded (see below).

| Change | Result | Decision |
|---|---|---|
| Event-driven completion waits (`awaitTerminal`, `GET ?waitMs=`) instead of 0.2 ms status polling | Removes a busy-polling load generator that competed with the engine for CPU (not separately re-measured on JDK 21) | **Kept**: also a client-facing API feature |
| Batch cap 64 → 256 + delta WAL records + shallow copy-on-write ("after") | Same throughput at ≤ 32 workers; +69% at 128 and **2.75× at 256 workers** (2,499 → 6,885 wf/s), where the old cap pinned every batch at 64 | **Kept** |
| Delta (patch) WAL records alone | WAL bytes per workflow 8,130 → 2,730 (−66%); replay verified byte-identical over 3,000 randomized transitions; v0.2 WALs still replay | **Kept** |
| Pipelined commit (execute the next batch during `fsync`) | −6% at 4 workers, −7% at 16, +1% at 128 | **Opt-in only** (`-Dconductor.pipelinedCommit=true`) |
| Partitioned journal (N independent WAL writers) | 2 shards: −24% at 4 workers, −11% at 128; 4 shards: −27% and −53%. Smaller batches cost more `fsync`s than parallel flushing saves on this SSD | **Opt-in** (`JOURNAL_SHARDS`), default 1 |
| Preallocated WAL file | No consistent `fsync` gain (platform-thread probe, unaffected by the JVM issue); complicates torn-tail recovery | **Rejected** |

## 2026-10-20 — The benchmark host lied twice before the JVM did

**Observation 1:** `fsync` latency on the laptop's DRAM-less NVMe moves between ~0.65 ms (after idle) and ~2.4 ms (sustained writing). Comparing a run with one taken an hour earlier was meaningless.

**Fix:** configurations are interleaved within each repetition, and a raw 1-thread `fsync` probe runs before every benchmark; its value is stored with the result.

**Observation 2:** the laptop switched to battery and entered Modern Standby for most of an afternoon of sweeps; one run "stalled" for seven minutes across a system sleep (Kernel-Power events 42/107/506).

**Fix:** the sweep holds a Windows execution-state request (no settings changed) and pauses while on battery.

## 2026-10-20 — JDK 25.0.2 virtual-thread timers stall for seconds (JDK-8370887)

**Observation:** with 10 ms simulated activities at 128 workers, throughput collapsed and p99 sat at exactly 10 s. A thread dump during the freeze showed all 128 workers parked inside `Thread.sleep(10)` and all carriers idle. A 30-line program with no project code reproduced it: on JDK 25.0.2, 10 ms virtual-thread sleeps woke after up to 9,991 ms once many longer timed parks were being cancelled early. JDK 21.0.2: worst 17 ms. Temurin 25.0.4.1: worst 25 ms.

**Cause:** JDK-8370887, "DelayScheduler.replace method may break the 4-ary heap in certain scenarios". Long polls and idle-worker waits are exactly the cancel-early pattern.

**Decision:** all JDK 25.0.2 measurements from this session were discarded; the study was rerun on JDK 21 (the Docker/CI runtime). The service logs a warning at startup on JDK 25 builds before 25.0.4.
