# Canonical benchmark

**Quote only the numbers on this page.** Every other figure in this repository was measured under different conditions (worker count, harness, journal sync mode, JDK) and exists to explain *how* the engine got here. The mapping table at the end says which is which.

## Configuration

| | |
|---|---|
| Engine | journal backend, shipped defaults: write-through WAL, batch cap 256, patch records, direct grants |
| Process | one JVM; 256 in-process workers |
| JVM | JDK 21.0.2, `-Xmx3g -XX:MaxGCPauseMillis=5` |
| Workload | three-step workflow: two ledger effects (separately journaled downstream service) and an echo; instant activities; every transition and effect durable before acknowledgment |
| Machine | laptop, Intel i5-11300H (4 cores / 8 threads), Samsung PM991a NVMe (DRAM-less, no power-loss protection), Windows 11, on AC, kept awake |
| Method | fresh JVM per run; configurations interleaved; 3 repetitions, median reported; disk `fsync` probe before each run. Open-loop runs offer a fixed fraction of the capacity measured immediately before them, measure latency from each workflow's intended arrival time (no coordinated omission), and exclude the first 20% as warm-up |
| Reproduce | `python scripts/latency_sweep.py --label canonical`, then `python scripts/plot_latency.py benchmarks/latency/canonical` |

## The numbers

| Metric | Value |
|---|---|
| **Peak throughput** (closed loop, 1,024 clients) | **≈ 6,600 workflows/s** (6,651 median; runs 6,084–6,858) |
| **Unloaded latency** (one workflow at a time) | **p50 0.98 ms, p99 4.0 ms** |
| **Latency at 50% of capacity** (≈ 2,750 workflows/s) | **p50 1.5 ms, p99 15 ms, p99.9 38 ms** |
| Latency at 75% of capacity | p50 2.0 ms, p99 108 ms |
| Latency at 90% of capacity | p50 2.3 ms, p99 705 ms |

One-line version: *≈ 6,600 durable workflows/s peak on a laptop; at half that load, p99 15 ms; unloaded p50 under 1 ms.*

![Latency vs load](latency.png)

| Offered load | p50 | p99 | p99.9 | p99, batch cap 64 |
|---:|---:|---:|---:|---:|
| 10% | 1.02 ms | 7.2 ms | 31 ms | 8.1 ms |
| 25% | 1.12 ms | 11.2 ms | 36 ms | 9.7 ms |
| 50% | 1.53 ms | 15.0 ms | 38 ms | 14.5 ms |
| 75% | 2.03 ms | 108 ms | 178 ms | 401 ms |
| 90% | 2.31 ms | 705 ms | 772 ms | 1,281 ms |

**Operating guidance:** keep sustained load at or below ~50% of capacity for a p99 in the low tens of milliseconds. The knee is between 50% and 75%.

## Where the tail comes from

The p99/p50 ratio widens with load: about 7× at 10% load, 10× at 50%, 300× at 90%. The components were measured separately in every run:

| Component (p99) | 10% load | 50% load | 90% load |
|---|---:|---:|---:|
| Journal durable write | 0.38 ms | 0.40 ms | 0.60 ms |
| Batch formation (first command → batch durable) | 0.47 ms | 0.70 ms | 5.3 ms |
| Longest GC pause in the run | 5 ms | 8 ms | 24 ms |
| Slowest 1% overlapping a GC pause / another JVM-OS stall / neither | 22% / 36% / 44% | 76% / 0% / 14% | 100% / 0% / 0% |

- **Batch formation is not the tail.** Up to 50% load, the device write and batch formation together are under ~1 ms at p99.
- **The tail is pauses.** These are GC pauses, and stalls of up to ~50 ms where the JVM or OS stopped a spinning probe thread on this shared laptop. Slow workflows overlap them far more often than the base rate: 14–42× for GC in the attribution runs.
- **Pauses are amplified by queueing as load rises.** A pause leaves a backlog, and the spare capacity available to drain it shrinks as utilization grows, roughly by load/(1 − load). That is why the same pauses cost ~15 ms at 50% load and hundreds of milliseconds at 90%.
- **"Neither" at low load** is mostly workflows that started just after a stall ended and queued behind its backlog. An interval-overlap test does not catch those.
- **GC tuning already applied:** `-XX:MaxGCPauseMillis=5` cut p99 at 2,000/s from 14–20 ms to 8–9 ms with unchanged p50 and CPU. Generational ZGC was tried and was worse here (p50 doubled).

## Does the batch cap trade tail latency for throughput?

No.
- **Below 50% load the caps are indistinguishable.** Median batches are 1–3 commands, far below either cap.
- **At 75–90% load cap 256 has 2–4× lower p99** (108 vs 401 ms; 705 vs 1,281 ms). The cap only binds while a backlog drains after a pause, and a larger cap drains it in fewer flushes.
- **Peak throughput barely moves** (6,651 vs 6,371/s, overlapping run ranges). The write-through journal makes each flush cheap enough that batches rarely fill at 256 workers.

## Durability caveat

These are write-through numbers on a drive without power-loss protection. The drive was shown to do real extra work for FUA writes (57–66 µs vs 4 µs cached), but a power-cut test was not run. See [durability](durability.md) for the evidence, the risk and the verification procedure.

## Other numbers in this repository, and why they differ

| Figure | Where | Conditions | Status |
|---|---|---|---|
| 143 wf/s | v0.1 docs | v0.1 engine, 8 workers, JDK 21, polling harness | historical |
| 653 → 1,041 wf/s | `benchmarks/superseded-jdk25.0.2` | JDK 25.0.2 (timer bug), 8 workers | superseded |
| 2,499 → 6,885 wf/s | [scaling](scaling.md) | before/after engine changes, 256 workers, **flush** mode, closed loop, default GC | explains the scaling work |
| 20 → 2,666 wf/s | [scaling](scaling.md) | 10 ms simulated activities, 1 → 256 workers, flush mode | explains worker-pool scaling |
| 4.87 → 4.24 → 1.12 ms p50 | [scaling § Latency](scaling.md#latency) | 1 client, 8 workers; direct grants, then write-through | explains the latency work |
| +38% / +22% throughput | [scaling § Latency](scaling.md#latency) | flush vs write-through, 64 / 256 workers, closed loop | explains write-through |
| 55 → 280 wf/s | [scaling](scaling.md) | 1–8 worker **processes** over HTTP, flush mode | explains the remote-worker protocol cost |
