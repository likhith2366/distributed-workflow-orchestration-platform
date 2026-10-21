"""Prints the latency tail and its candidate causes for one or more Benchmark result files.

    python scripts/tail_report.py benchmarks/latency/open-loop/*.json

Columns: offered and achieved rate; workflow p50/p99/p99.9; and the components a tail can come
from: start (submission until durable), worker dispatch, commit wait, effect wait, raw journal
write latency, batch size and batch open-to-durable time, and GC pauses.
"""
import json
import sys


def us(v):
    return f"{v / 1000:.2f}"


def main():
    print("| file | offered | achieved | p50 ms | p99 ms | p99.9 ms | start p99 | dispatch p99 ms | commit p99 | effect p99"
          " | write p99 | batch p50/p99 | batch open-to-durable p99 | GC pauses (max ms) |")
    for path in sys.argv[1:]:
        r = json.load(open(path))
        e, w, l = r["engineMetrics"], r["workersMetrics"], r["ledgerMetrics"]
        print(f"| {path.replace(chr(92), '/').split('/')[-1]} | {r.get('offeredRate', 0):,.0f} | {r['steadyWorkflowsPerSecond']:,.0f}"
              f" | {us(r['workflowP50Micros'])} | {us(r['workflowP99Micros'])} | {us(r.get('workflowP999Micros', 0))}"
              f" | {us(r.get('startP99Micros', 0))} | {w['dispatchP99Ms']}"
              f" | {us(w['commitMicrosP99Ms'])} | {us(w['effectMicrosP99Ms'])}"
              f" | {us(e['writeLatency']['writeMicrosP99Ms'])}/{us(l['writeLatency']['writeMicrosP99Ms'])}"
              f" | {e['writer']['batchSizeP50Ms']}/{e['writer']['batchSizeP99Ms']}"
              f" | {us(e['writer']['batchOpenToDurableMicrosP99Ms'])}"
              f" | {r['gc']['pauses']} ({r['gc']['maxPauseMs']}) |")


if __name__ == "__main__":
    main()
