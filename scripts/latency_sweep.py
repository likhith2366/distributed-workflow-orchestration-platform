"""Canonical benchmark: peak throughput, then the latency-vs-load curve up to it (local, no AWS).

    ./mvnw -Dconsole.skip=true -DskipTests test-compile
    python scripts/latency_sweep.py --label canonical

Canonical configuration (everything the resume numbers refer to):
  journal backend, current defaults (write-through, batch cap 256, patch records, direct grants);
  one JVM, 256 in-process workers; three-step workflow with two ledger effects + echo, instant
  activities; JDK 21 with -Xmx3g -XX:MaxGCPauseMillis=5.

Steps, every run a fresh JVM, configurations interleaved inside each repetition:
  1. peak: closed loop, 1024 clients, median of 3 per config;
  2. unloaded: closed loop, 1 client (p50/p99 of one workflow at a time);
  3. open loop at fixed fractions of capacity measured immediately before each run in the same
     configuration (the laptop's SSD drifts between ~0.65 and ~2.4 ms per write within minutes, so
     a fraction of an earlier peak can silently be overload), latency from intended arrival time,
     first 20% excluded as warm-up, with tail attribution (GC / other stall / neither).
The comparison config ("cap64") changes only the batch cap, to measure its tail-latency cost.
"""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scaling_sweep import keep_awake, probe, wait_for_ac  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
JAVA = str(Path(os.environ["JAVA_HOME"]) / "bin" / "java") if "JAVA_HOME" in os.environ else "java"
JVM = ["-Xmx3g", "-XX:MaxGCPauseMillis=5"]
WORKERS = 256


def run(cp, flags, args, out):
    wait_for_ac()
    disk = probe(cp)
    subprocess.run([JAVA, *JVM, *flags, "-cp", cp, "dev.conductor.tools.Benchmark", "--workers", str(WORKERS), *args,
                    "--out", str(out)], cwd=ROOT, check=True, stdout=subprocess.DEVNULL)
    r = json.loads(out.read_text())
    r["diskProbeMs"] = disk
    out.write_text(json.dumps(r))
    return r


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--label", default="canonical")
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--fractions", default="0.1,0.25,0.5,0.75,0.9")
    p.add_argument("--seconds", type=float, default=10)
    p.add_argument("--classes", default="target")
    args = p.parse_args()
    keep_awake()
    base = ROOT / args.classes
    cp = os.pathsep.join([str(base / "classes"), str(base / "test-classes")])
    folder = ROOT / "benchmarks" / "latency" / args.label
    folder.mkdir(parents=True, exist_ok=True)
    configs = {"default": [], "cap64": ["-Dconductor.batchSize=64"]}
    summary = {"label": args.label, "workers": WORKERS, "jvm": JVM, "configs": configs, "peak": {}, "unloaded": {}, "openLoop": []}

    peaks = {name: [] for name in configs}
    for rep in range(args.reps):
        for name, flags in configs.items():
            r = run(cp, flags, ["--workflows", str(64 * WORKERS), "--clients", str(4 * WORKERS)], folder / f"peak-{name}-r{rep}.json")
            peaks[name].append(r["steadyWorkflowsPerSecond"])
            print(f"peak {name} r{rep}: {r['steadyWorkflowsPerSecond']:,.0f}/s", flush=True)
    for name in configs:
        summary["peak"][name] = {"median": round(statistics.median(peaks[name])), "runs": [round(x) for x in peaks[name]]}

    unloaded = {name: [] for name in configs}
    for rep in range(args.reps):
        for name, flags in configs.items():
            r = run(cp, flags, ["--workflows", "3000", "--clients", "1"], folder / f"unloaded-{name}-r{rep}.json")
            unloaded[name].append((r["steadyP50Micros"], r["steadyP99Micros"]))
    for name in configs:
        summary["unloaded"][name] = {"p50Ms": round(statistics.median(x[0] for x in unloaded[name]) / 1000, 2),
                                     "p99Ms": round(statistics.median(x[1] for x in unloaded[name]) / 1000, 2)}
    print("unloaded", summary["unloaded"], flush=True)

    for fraction in [float(f) for f in args.fractions.split(",")]:
        rows = {name: [] for name in configs}
        for rep in range(args.reps):
            for name, flags in configs.items():
                capacity = run(cp, flags, ["--workflows", str(32 * WORKERS), "--clients", str(4 * WORKERS)],
                               folder / f"capacity-{name}-f{fraction}-r{rep}.json")["steadyWorkflowsPerSecond"]
                rate = round(capacity * fraction)
                count = max(2000, int(rate * args.seconds / 0.8))
                r = run(cp, flags, ["--workflows", str(count), "--rate", str(rate), "--hiccup", "true"],
                        folder / f"open-{name}-f{fraction}-r{rep}.json")
                r["capacityJustBefore"] = capacity
                rows[name].append(r)
        for name in configs:
            v = rows[name]
            med = lambda key: statistics.median(x[key] for x in v)  # noqa: E731
            a = [x["tailAttribution"] for x in v]
            row = {
                "config": name, "fractionOfCapacity": fraction,
                "capacityJustBefore": [round(x["capacityJustBefore"]) for x in v],
                "offered": [round(x["offeredRate"]) for x in v],
                "achieved": [round(x["measuredRate"]) for x in v],
                "p50Ms": round(med("steadyP50Micros") / 1000, 2), "p99Ms": round(med("steadyP99Micros") / 1000, 2),
                "p999Ms": round(med("steadyP999Micros") / 1000, 2),
                "p99Runs": [round(x["steadyP99Micros"] / 1000, 2) for x in v],
                "batchP50": statistics.median(x["engineMetrics"]["writer"]["batchSizeP50Ms"] for x in v),
                "batchP99": statistics.median(x["engineMetrics"]["writer"]["batchSizeP99Ms"] for x in v),
                "writeP99Ms": round(statistics.median(x["engineMetrics"]["writeLatency"]["writeMicrosP99Ms"] for x in v) / 1000, 3),
                "batchOpenToDurableP99Ms": round(statistics.median(x["engineMetrics"]["writer"]["batchOpenToDurableMicrosP99Ms"] for x in v) / 1000, 3),
                "dispatchP99Ms": statistics.median(x["workersMetrics"]["dispatchP99Ms"] for x in v),
                "tailGc": round(statistics.median(t["tailOverlappingGcPause"] for t in a), 2),
                "tailStall": round(statistics.median(t["tailOverlappingOtherStall"] for t in a), 2),
                "tailNeither": round(statistics.median(t["tailOverlappingNeither"] for t in a), 2),
                "gcMaxPauseMs": max(x["gc"]["maxPauseMs"] for x in v),
                "diskProbeMs": [x["diskProbeMs"] for x in v],
            }
            summary["openLoop"].append(row)
            print(json.dumps(row), flush=True)
        (folder / "summary.json").write_text(json.dumps(summary, indent=2))
    (folder / "summary.json").write_text(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
