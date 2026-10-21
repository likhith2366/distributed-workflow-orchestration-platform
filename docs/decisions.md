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

## 2026-10-20 — Latency: remove a serial flush, keep every durability promise

**Observation:** unloaded latency (one client) was ~7 serial `fsync`s per three-step workflow; CPU was negligible. One of those flushes existed only because an idle worker was woken after a start was durable and then committed its own claim.

**Decision:** direct grants. Idle workers park in the engine, and the writer assigns them due tasks inside the batch that made the tasks due; a grant is released after that batch's flush. p50 −13% and p99 −12% at one client, −11% p50 at four (interleaved, 5 reps, `benchmarks/latency/`).

**Rejected:** speculative execution of the next step before the previous completion is durable (≈ −30% latency, but downstream effects could precede a durable success of their dependency), and asynchronous effect acknowledgment (breaks effect-before-completion). Both trade correctness for speed; neither was implemented.

**Also found:** the Maven pom hard-coded the Surefire `argLine`, silently ignoring `-DargLine`, so an earlier "tests pass in pipelined mode" check had not actually run pipelined mode for the engine tests. The pom now takes `-Dconductor.testArgs=...`, and all four engine modes (default, pipelined, wake-then-claim, previous-engine) are run against the same 37 tests.

## 2026-10-20 — Write-through WAL writes instead of write-then-flush

**Observation:** after direct grants, unloaded latency was still ~6 serial durable writes at ~0.65 ms each. `force()` on Windows is `FlushFileBuffers`, which drains the device's whole volatile cache. A device probe measured 0.63 ms for append + flush and 0.11 ms for append with `DSYNC` (FUA writes).

**Decision:** open journals with `DSYNC` by default. Microsoft documents that write-through also flushes file metadata (so appends that grow the file stay recoverable), and Linux `O_DSYNC` likewise covers the metadata needed to read data back. Recovery semantics are unchanged; a recovery-time truncation of a torn tail is now also forced. p50 at one client 4.20 → 1.12 ms; throughput +22–38% under saturation; 200/200 crash recoveries.

**Rejected:** preallocated segments (a further ~2× in the probe) because a torn write inside preallocated space is indistinguishable from corruption of the last acknowledged frame, which the journal currently fails closed on.

**Escape hatch:** `JOURNAL_SYNC=flush` for storage stacks that do not honor FUA.

## 2026-10-20 — One canonical benchmark; latency measured open-loop to saturation

**Problem:** the docs quoted throughput from five different harnesses, worker counts, task types and sync modes, and latency was measured only at low concurrency, in closed loop, where it mostly reflects the number of clients.

**Decision:** [benchmark.md](benchmark.md) defines one canonical configuration and is the only source for headline numbers. Latency is measured open-loop at fixed fractions of capacity, from intended arrival time, excluding warm-up.

**Method correction:** the first open-loop sweep offered fractions of a peak measured earlier while the SSD was in its fast state. When the disk slowed, "75% of peak" became overload and p99 reached seconds. It was discarded; each open-loop run now offers a fraction of capacity measured immediately before it.

**Findings:**
- The tail is GC and OS pauses amplified by queueing (attribution: slow workflows overlap GC pauses 14–42× more often than the base rate). Batch formation is under 1 ms at p99 up to 50% load.
- `-XX:MaxGCPauseMillis=5` halved p99 at equal p50 and CPU and is now the shipped JVM setting.
- Generational ZGC was worse on this 4-core laptop.
- The larger batch cap lowers high-load p99 rather than raising it.
- The first ~300 arrivals of every run carry JIT warm-up latency; including them had made p99 look like 275–330 ms.

## 2026-10-21 — Two headline numbers, and a direct test of the batching tail

**Review feedback:** quote two numbers with conditions stated once; record p50/p99 at every point of the worker sweep; split each request's time into waiting for its batch versus waiting for the write, to test whether the tail is batch formation.

**Done:**
- `DurableDispatcher` now records, per command, queue wait (including waiting out an in-flight flush), batch formation and the durable write, plus the shares for the slowest 1%.
- The canonical sweep (concurrency = workers, 1 → 256, write-through, `MaxGCPauseMillis=5`, 3 reps, disk in its fast state throughout) gives the two numbers: **10,100 workflows/s at 256 concurrent; p50 1.36 ms / p99 5.7 ms at one.**
- **Finding:** at 1–2 concurrent the tail is the device write (53–60% of the slowest commands' time); from 16 up it is batching (75–96%), with queue wait matching a "one full batch ahead plus one write" prediction through 32 concurrent and formation within 2–3× of batch size × per-command cost. At 128–256, GC pauses (4–7% of wall time) turn the following batches into full 256-command batches and inflate both components.
- The v0.1 (143 wf/s, a harness artifact) and JDK 25.0.2 result folders were removed from the tree.

**Not adopted from the feedback:** the suggested pair "6,885 wf/s and 1.12 / 3.60 ms" mixes a flush-mode, 1,024-client run with an 8-worker, default-GC run. Both headline numbers now come from one sweep instead.
