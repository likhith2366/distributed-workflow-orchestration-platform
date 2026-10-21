"""Worker-count scaling sweep for the journal engine (local only, no AWS).

    ./mvnw -Dconsole.skip=true -DskipTests test-compile
    python scripts/scaling_sweep.py --label final \
        --config "before=-Dconductor.batchSize=64 -Dconductor.deepCopyTransitions=true -Dconductor.fullProjectionRecords=true" \
        --config "after=" [--workers 1,2,...,256] [--activity-ms 0,10] [--reps 3]

Every run is a fresh JVM. Configurations are interleaved inside each repetition, so slow drift in
the machine (this laptop's SSD fsync latency moves 2-4x over tens of minutes) hits all of them
alike. Results land in benchmarks/scaling/<label>/; summary.json holds per-config medians and the
writer counters that explain where scaling stops.
"""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
JAVA = str(Path(os.environ["JAVA_HOME"]) / "bin" / "java") if "JAVA_HOME" in os.environ else "java"


def workload(activity, workers):
    """Sized for several seconds of steady state at each point."""
    if activity == 0:
        return 3000 if workers <= 8 else 250 * workers
    return max(400, 300 * workers)


def keep_awake():
    """Hold a Windows execution-state request so the laptop cannot sleep or enter Modern Standby.

    The first sweeps on the reference laptop silently ran on battery in Modern Standby (screen off):
    fsync latency tripled and one run stalled for minutes during a sleep. The request lasts only
    as long as this process; no power settings are changed.
    """
    if sys.platform == "win32":
        import ctypes
        ES_CONTINUOUS, ES_SYSTEM_REQUIRED, ES_DISPLAY_REQUIRED = 0x80000000, 0x1, 0x2
        ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED)


def on_ac_power():
    if sys.platform != "win32":
        return True
    import ctypes

    class Status(ctypes.Structure):
        _fields_ = [("ac", ctypes.c_byte), ("flag", ctypes.c_byte), ("pct", ctypes.c_byte), ("saver", ctypes.c_byte),
                    ("life", ctypes.c_ulong), ("full", ctypes.c_ulong)]
    status = Status()
    ctypes.windll.kernel32.GetSystemPowerStatus(ctypes.byref(status))
    return status.ac == 1


def wait_for_ac():
    warned = False
    while not on_ac_power():
        if not warned:
            print("on battery: pausing until AC power returns", file=sys.stderr, flush=True)
            warned = True
        time.sleep(10)


def probe(cp):
    """Raw single-thread fsync latency right now: the disk state each run was measured under."""
    out = subprocess.run([JAVA, "-cp", cp, "dev.conductor.tools.FsyncProbe", "--threads", "1", "--seconds", "1"],
                         cwd=ROOT, check=True, capture_output=True, text=True).stdout
    return round(json.loads(out.strip().splitlines()[-1])["meanFsyncMs"], 3)


def row(config, activity, workers, clients, workflows, runs):
    mid = sorted(runs, key=lambda r: r["steadyWorkflowsPerSecond"])[len(runs) // 2]
    e = mid["engineMetrics"]
    w, j = e["writer"], e["journal"]
    return {
        "config": config, "activityMs": activity, "workers": workers, "clients": clients, "workflows": workflows,
        "steadyWfs": [round(r["steadyWorkflowsPerSecond"], 1) for r in runs],
        "medianSteadyWfs": round(statistics.median(r["steadyWorkflowsPerSecond"] for r in runs), 1),
        "p50Ms": mid["workflowP50Ms"], "p99Ms": mid["workflowP99Ms"], "cpuCores": round(mid["processCpuCores"], 2),
        "meanBatch": round(w["meanBatch"], 2), "maxBatch": w["maxBatch"],
        "flushShare": round(w["flushMs"] / 1000 / mid["seconds"], 3),
        "executeShare": round(w["executeMs"] / 1000 / mid["seconds"], 3),
        "fsyncMsPerFlush": round(j["writeAndForceMs"] / max(1, j["flushes"]), 3),
        "transitionsPerWorkflow": round(e["durableTransitions"] / workflows, 2),
        "walBytesPerWorkflow": round(j["bytes"] / workflows),
        "p50Micros": mid.get("steadyP50Micros", mid.get("workflowP50Micros")),
        "p99Micros": mid.get("steadyP99Micros", mid.get("workflowP99Micros")),
        "p50MicrosRuns": [r.get("steadyP50Micros") for r in runs],
        "p99MicrosRuns": [r.get("steadyP99Micros") for r in runs],
        "batchSizeP50": w.get("batchSizeP50Ms"), "batchSizeP99": w.get("batchSizeP99Ms"),
        "executeMicrosPerCommand": round(w["executeMs"] * 1000 / max(1, w["commands"]), 1),
        "engineCommand": w.get("commandLatency"),
        "ledgerCommand": mid["ledgerMetrics"].get("writer", {}).get("commandLatency"),
        "gc": mid.get("gc"),
        "diskProbeMs": [r.get("diskProbeMs") for r in runs],
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--label", required=True)
    p.add_argument("--config", action="append", required=True, help="name=JVM flags (repeatable)")
    p.add_argument("--workers", default="1,2,4,8,16,32,64,128")
    p.add_argument("--activity-ms", default="0,10")
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--clients-per-worker", type=float, default=4, help="closed-loop clients per worker (1 = concurrency equals workers)")
    p.add_argument("--min-clients", type=int, default=64)
    p.add_argument("--classes", default="target", help="dir with classes/ and test-classes/ (snapshot it before long sweeps)")
    args = p.parse_args()
    keep_awake()
    configs = [c.split("=", 1) for c in args.config]
    base = ROOT / args.classes
    cp = os.pathsep.join([str(base / "classes"), str(base / "test-classes")])
    folder = ROOT / "benchmarks" / "scaling" / args.label
    folder.mkdir(parents=True, exist_ok=True)
    summary = []
    for activity in [int(a) for a in args.activity_ms.split(",")]:
        for workers in [int(w) for w in args.workers.split(",")]:
            clients, workflows = max(args.min_clients, int(args.clients_per_worker * workers)), workload(activity, workers)
            runs = {name: [] for name, _ in configs}
            for rep in range(args.reps):
                for name, flags in configs:
                    wait_for_ac()
                    disk = probe(cp)
                    out = folder / f"{name}-a{activity}-w{workers}-r{rep}.json"
                    subprocess.run([JAVA, "-Xmx2g", *flags.split(), "-cp", cp, "dev.conductor.tools.Benchmark",
                                    "--workflows", str(workflows), "--clients", str(clients), "--workers", str(workers),
                                    "--activity-ms", str(activity), "--out", str(out)],
                                   cwd=ROOT, check=True, stdout=subprocess.DEVNULL)
                    result = json.loads(out.read_text())
                    result["diskProbeMs"] = disk
                    out.write_text(json.dumps(result))
                    runs[name].append(result)
            for name, _ in configs:
                r = row(name, activity, workers, clients, workflows, runs[name])
                summary.append(r)
                print(json.dumps(r), flush=True)
            (folder / "summary.json").write_text(json.dumps(
                {"label": args.label, "configs": dict(configs), "points": summary}, indent=2))


if __name__ == "__main__":
    main()
