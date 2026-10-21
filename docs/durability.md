# Durability: what is verified, what is trusted, and how to check the rest

**Short answer to "did you verify that the SSD honors write-through, or are you trusting the spec?":**
- **The OS-level guarantee: verified**, from Microsoft's and Linux's documentation.
- **The hardware: not verified.** That needs a power-loss rig, which I don't have.
- **But this is not a new risk.** The previous journal's `fsync` depended on the drive obeying a cache-flush command; the write-through journal depends on it obeying the force-unit-access flag. A drive that does not honor FUA generally does not honor flush either, so the same trust moved from one command to the other. If the drive lies, both versions were already unsafe.
- **`JOURNAL_SYNC=flush`** remains for deployments that trust flush but not FUA.

Two cheap checks were also done:

| Check | Result |
|---|---|
| Is Windows' "turn off write-cache buffer flushing" enabled for this drive? (If it were, `FlushFileBuffers` would not reach the device and flush-mode numbers would be meaningless.) | **No.** The per-device registry policy (`CacheIsPowerProtected`, `UserWriteCacheSetting` under the drive's `Device Parameters\Disk` key) is unset, i.e. Windows defaults. Independently, a flush measurably costs 609–666 µs against 4 µs for a cached write, which could not happen if flushes were being skipped. |
| Does the drive advertise power-loss protection? | **No.** Windows does not mark the cache as power-protected. Samsung documents PLP for its data-center drives (e.g. PM893/PM897), not for the PM991a client drive; consumer and client NVMe almost never has it. |

## What the journal asks for

- **Default (`JOURNAL_SYNC=write-through`):** the WAL is opened with `DSYNC`.
  - On Windows that is `FILE_FLAG_WRITE_THROUGH`, which NTFS issues to the device as FUA (Force Unit Access) writes ([Old New Thing](https://devblogs.microsoft.com/oldnewthing/?p=95505)).
  - Microsoft documents that write-through also covers file metadata such as the new size ([file caching](https://learn.microsoft.com/windows/win32/fileio/file-caching)).
  - On Linux it is `O_DSYNC`.
- **`JOURNAL_SYNC=flush`:** write, then `force(true)`, i.e. `FlushFileBuffers` / `fsync`, which asks the device to drain its whole volatile write cache.

Both are *requests*. Whether the data is on non-volatile media when the request returns is up to the device firmware.

## What was measured on the reference drive

The drive is a Samsung MZVLQ512HBLU (PM991a), a DRAM-less client NVMe SSD. Client drives of this class do not advertise power-loss protection. Reading the device's write-cache and power-protection flags needs administrator rights, which the benchmark session did not have, so the analysis assumes **a volatile write cache and no PLP**.

`FsyncProbe` timed single-threaded writes over 4 separate runs (recorded run: `benchmarks/durability/`):

| Write | Latency | Interpretation |
|---|---:|---|
| Direct (bypassing the OS cache), no FUA | **4 µs** | acknowledged from the drive's volatile cache (on this DRAM-less drive, host memory buffer or SRAM) |
| Direct, with FUA | **57–66 µs** | 14–16× slower: consistent with an actual NAND program |
| Buffered + write-through (the journal's mode) | 110–123 µs | FUA data write plus NTFS metadata |
| Append + `FlushFileBuffers` (flush mode) | 609–666 µs | whole-cache flush |

**What this shows:** the drive does substantially more work for a FUA write than for a cached write, so it is not simply acknowledging FUA from volatile cache. A drive that ignored FUA would show ~4 µs for both.

**What this does not show:** that the data survives power loss. Firmware could program the data yet defer the mapping-table update that makes it findable after a reboot. Only cutting power while acknowledged writes are in flight can show that, and that test **has not been run**: there was no second machine, and no consent to hard-power-cycle the laptop repeatedly.

The process-level crash harness (200 seeded `kill -9` equivalents, 200/200 recovered, 0 duplicate effects) proves recovery logic under **process** crashes. There, the OS page cache survives. It says nothing about the device's behavior on **power** loss.

## The risk, stated plainly

If the drive acknowledged a write that was not yet on non-volatile media, a power cut could lose the last acknowledged transitions. Flush mode has the same class of risk: some consumer drives have also been reported to acknowledge cache flushes early. The journal's checksums mean a lost or torn tail is detected, and the torn final frame is truncated. They cannot resurrect data the device never persisted.

## Mitigations, in order of strength

1. **Run the power-loss check on the target hardware before trusting it:** `PowerLossCheck` (below), at least 20 power cuts in each sync mode.
2. **Use storage with power-loss protection** for anything that matters: enterprise NVMe with PLP capacitors, or cloud block storage. A PLP drive also makes durable writes far cheaper.
3. **Don't depend on one device:** the DynamoDB backend replicates every transition across availability zones before acknowledging, removing single-drive durability from the equation.
4. **`JOURNAL_SYNC=flush`** where FUA is known to be unreliable (old SATA/IDE stacks, some virtualized disks).

## Power-loss check: procedure

`src/test/java/dev/conductor/tools/PowerLossCheck.java` writes through the production `Journal` class and reports each sequence number to a second machine **only after** the journal acknowledged it as durable.

```sh
# Machine B (keeps power):
java -cp target/classes:target/test-classes dev.conductor.tools.PowerLossCheck listen --port 9123

# Machine A (under test), optionally with JOURNAL_SYNC=flush:
java -cp target/classes:target/test-classes dev.conductor.tools.PowerLossCheck write --dir /data/plc --host machine-b --port 9123
#   ... then cut power to machine A (pull the cord / hold the power button).

# Machine A after reboot; RUN and N from machine B's last line for that run:
java -cp target/classes:target/test-classes dev.conductor.tools.PowerLossCheck verify --dir /data/plc --run RUN --acked N
```

`verify` passes only if every acknowledged sequence number is present, contiguously, after replay. Records written after the last acknowledgment may or may not survive, and either outcome is allowed. A single failure means the drive acknowledged writes it did not persist, so do not use that drive/sync-mode combination for the journal.
