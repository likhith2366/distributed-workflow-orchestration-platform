# Interview preparation and evidence-backed resume language

Only use these bullets after you can explain, run, and modify the implementation yourself.

**Distributed Workflow Orchestration Platform | Java 21, Spring Boot, React + TypeScript, DynamoDB, SQS, S3, EventBridge, AWS CDK**

- Built a durable DAG workflow engine (Java 21, Spring Boot, React/TypeScript) with fenced worker leases, persisted retries, timers and approvals, and reverse-order saga compensation. One pure state machine backs a group-committed write-ahead log and a DynamoDB/SQS/S3/EventBridge backend with a transactional outbox.
- Reached **10,100 durable workflows/s** at 256 concurrent workflows, with **p50 1.36 ms / p99 5.7 ms** at one (JDK 21, 4-core laptop, NVMe, write-through journal, every transition durable). Got there by finding a batch cap that pinned group commit, writing field-level WAL patches, granting work inside the committing batch, and switching to write-through (FUA) journal writes, each measured A/B.
- Built the throughput-latency curve and decomposed every journal command into queue wait, batch formation and write. The tail is the device write at low concurrency and batching from 16 up, predictable from batch size within 2-3x; GC pauses create the full batches at the top end. Tuning G1's pause target roughly halved p99.
- Traced a 10-second throughput collapse to a JDK 25.0.2 virtual-thread timer bug (JDK-8370887) with a 30-line reproduction.
- Validated recovery across **200 seeded process kills: 200/200 completed, zero duplicate effects**.

Quote only the canonical numbers ([benchmark.md](benchmark.md)), always with "on a laptop" and "durable". Keep "local" and "LocalStack" in these claims until the CDK stack has been deployed and measured on AWS. Do not quote DynamoDB-backend throughput: none has been measured on real AWS.

**"Did you verify the SSD honors write-through, or trust the spec?"** I verified the OS-level guarantee from Microsoft's and Linux's documentation, and checked that Windows' "turn off write-cache buffer flushing" is off for this drive (the registry policy is unset, and a flush measurably costs ~0.6 ms, so flushes reach the device). I could not verify the hardware: that needs a power-loss rig. I did measure that the drive takes 57-66 us for a FUA write against 4 us for a cached one, so it is not simply acking FUA from cache. The drive does not advertise power-loss protection; consumer NVMe almost never does. Crucially, this is not a new risk: the previous implementation's fsync depended on the drive obeying a flush command, and the new one depends on it obeying the force-unit-access flag. A drive that lies about one generally lies about the other. If it lies, both versions were already unsafe. JOURNAL_SYNC=flush remains for deployments that trust flush but not FUA, and PowerLossCheck is ready for a real power-cut test.

**"What does p99 look like at peak?"** 50 ms at 10,100 workflows/s (256 concurrent); ~30 ms from 32 to 128 concurrent while throughput doubles. Open loop, p99 stays at ~15 ms up to 50% of capacity and rises sharply past 75%. Per-command decomposition says the tail is batching (queue wait + formation, 75-96% of the slowest commands from 16 concurrent up), inflated at the top by GC pauses that turn the next batches into full 256-command ones. The batch cap is the knob; a larger cap actually lowers high-load p99.

## Five-minute walkthrough

1. Start the console; submit the retail workflow. Show the DAG view, the payment's second attempt, and fraud-review approval.
2. Run the compensation demo. Explain why running branches drain before undo starts.
3. Kill a worker during the long activity; show attempt 2 and fence 2.
4. Open `core/Workflow.java`: one state machine, two stores.
5. Walk through one DynamoDB transition: conditional META, event append, outbox, counter. Then show the relay test.
6. Show `docs/scaling.png`: where each curve flattens and the counter that explains it (batch size, `fsync` time, commit wait).
7. Close with what is unmeasured: AWS.

## Questions worth rehearsing

**Why not simply say exactly-once?** Delivery and attempts repeat. Duplicate business effects are prevented only where the downstream system atomically records the idempotency key with the effect, as the example ledger does.

**What if the original worker wakes up?** Its fence or lease no longer validates, so its completion is rejected. Its external call may still happen; downstream idempotency covers that.

**Why an outbox?** "Commit, then send" loses the send if the process dies in between. Writing the message intent in the same transaction makes it durable. The relay delivers at least once, and conditional claims make duplicates harmless.

**How do you avoid a hot coordinator?** There isn't one in DynamoDB mode. Each execution is its own optimistic-concurrency unit (`version` condition), API tasks are stateless, and workers scale on queue depth. Contention exists only inside one execution with many parallel branches.

**How do long timers work?** Up to 15 minutes, SQS `DelaySeconds`. Beyond that, the outbox item's sort key begins with its due time; the relay picks it up once it is within SQS range.

**Whose clock decides a lease?** In journal mode, the coordinator's. In DynamoDB mode, the writing process's. Fencing keeps writes safe under skew; skew can only cause an early reclaim.

**Where does it stop scaling?** With instant activities, at the durable writer: throughput is commands per `fsync` divided by `fsync` time, so it grows only while batches can grow. With 10 ms activities, efficiency falls to 51% at 256 workers; per-step timing shows the engine's share is the commit wait (1.2 → 6.4 ms), the rest is Windows' 15.6 ms timer tick stretching the simulated activity.

**Why not partition the journal?** I measured it: on this SSD, parallel `fsync`s raise aggregate flush rate less than splitting the load shrinks each batch, so 4 shards were 27–53% slower. It stays as an option for hardware where parallel flushes are cheap.

**What actually surprised you?** Several things, all in `decisions.md`: the JDK 25.0.2 timer bug, the laptop entering Modern Standby mid-benchmark, Windows held a file lock briefly after process exit. Server-side long-polls from a killed worker claimed its task again (fixed by capping waits at a third of the lease). And a benchmark harness hid an engine speed-up by becoming the bottleneck.

**Why is LocalStack so slow?** It serializes DynamoDB transactions; I measured that directly before drawing conclusions. It proves behavior, not capacity.

## Ownership checklist

Understand `Workflow.valid`, `Workflow.advance`, `JournalEngine.completeAndClaim`, `DynamoEngine.mutate`/`prepare`, and the outbox relay test. Add a new idempotent activity yourself, explain its failure boundary, and run the measurements on your own machine before presenting the project as your work.
