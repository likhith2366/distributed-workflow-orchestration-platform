"""Multi-process scaling sweep: 1 coordinator JVM + P worker JVMs over loopback HTTP (local, no AWS).

    ./mvnw -Dconsole.skip=true -DskipTests package
    python scripts/cluster_sweep.py --label cluster [--processes 1,2,4,8] [--workers-per-process 4] [--activity-ms 10]

Each point starts a fresh cluster. JOURNAL_SHARDS for the coordinator comes from --shards.
"""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess

ROOT = Path(__file__).resolve().parents[1]
JAVA = str(Path(os.environ["JAVA_HOME"]) / "bin" / "java") if "JAVA_HOME" in os.environ else "java"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--label", required=True)
    p.add_argument("--processes", default="1,2,4,8")
    p.add_argument("--workers-per-process", type=int, default=4)
    p.add_argument("--activity-ms", default="0,10")
    p.add_argument("--shards", type=int, default=1)
    p.add_argument("--reps", type=int, default=2)
    p.add_argument("--classes", default="target", help="directory with classes/, test-classes/ and conductor.jar")
    args = p.parse_args()
    base = ROOT / args.classes
    cp = os.pathsep.join([str(base / "classes"), str(base / "test-classes")])
    folder = ROOT / "benchmarks" / "scaling" / args.label
    folder.mkdir(parents=True, exist_ok=True)
    summary = []
    env = dict(os.environ, JOURNAL_SHARDS=str(args.shards))
    for activity in [int(a) for a in args.activity_ms.split(",")]:
        for processes in [int(x) for x in args.processes.split(",")]:
            workers = processes * args.workers_per_process
            clients = max(64, 4 * workers)
            workflows = 4000 if activity == 0 else max(400, 250 * workers)
            runs = []
            for rep in range(args.reps):
                out = folder / f"a{activity}-p{processes}-r{rep}.json"
                subprocess.run([JAVA, "-Xmx1g", "-cp", cp, "dev.conductor.tools.ClusterBenchmark", "--jar", str(base / "conductor.jar"),
                                "--processes", str(processes), "--workers-per-process", str(args.workers_per_process),
                                "--workflows", str(workflows), "--clients", str(clients), "--activity-ms", str(activity),
                                "--out", str(out)], cwd=ROOT, env=env, check=True, stdout=subprocess.DEVNULL)
                runs.append(json.loads(out.read_text()))
            mid = sorted(runs, key=lambda r: r["steadyWorkflowsPerSecond"])[len(runs) // 2]
            engine = mid["coordinatorMetrics"]["engine"]
            row = {
                "activityMs": activity, "processes": processes, "workers": workers, "clients": clients,
                "shards": args.shards, "steadyWfs": [round(r["steadyWorkflowsPerSecond"], 1) for r in runs],
                "medianSteadyWfs": round(statistics.median(r["steadyWorkflowsPerSecond"] for r in runs), 1),
                "p50Ms": mid["workflowP50Ms"], "p99Ms": mid["workflowP99Ms"],
                "allProcessesCpuCores": round(mid["allProcessesCpuCores"], 2),
                "meanBatch": round(engine["writer"]["meanBatch"], 2),
            }
            summary.append(row)
            print(json.dumps(row), flush=True)
    (folder / "summary.json").write_text(json.dumps({"label": args.label, "kind": "cluster", "points": summary}, indent=2))


if __name__ == "__main__":
    main()
