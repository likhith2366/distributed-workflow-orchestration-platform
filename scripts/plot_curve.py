"""Throughput-latency curve from a scaling_sweep.py summary: one point per concurrency level.

    python scripts/plot_curve.py benchmarks/scaling/canonical-curve --out docs/curve.svg

Panel 1: p50 and p99 against achieved throughput, each point labelled with its concurrency (the knob
that moves along the curve). Panel 2: where the slowest 1% of engine commands spent their time
(queue wait / batch formation / durable write) at each concurrency.
"""
import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter, PercentFormatter  # noqa: E402

INK, INK_2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e6e5e1", "#fcfcfb"
P50, P99 = "#2a78d6", "#eb6834"
QUEUE, FORM, WRITE = "#2a78d6", "#eb6834", "#1baf7a"  # reference palette slots 1-3


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("folder")
    ap.add_argument("--out", default="docs/curve.svg")
    args = ap.parse_args()
    pts = sorted(json.loads((Path(args.folder) / "summary.json").read_text())["points"], key=lambda p: p["workers"])
    plt.rcParams.update({"font.family": "sans-serif", "font.size": 10, "axes.edgecolor": GRID,
                         "axes.labelcolor": INK_2, "xtick.color": INK_2, "ytick.color": INK_2})
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 5), facecolor=SURFACE, gridspec_kw={"width_ratios": [1.25, 1]})
    for ax in (ax1, ax2):
        ax.set_facecolor(SURFACE)
        ax.grid(True, color=GRID, linewidth=1)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
    xs = [p["medianSteadyWfs"] for p in pts]
    for key, color, name in (("p50Micros", P50, "p50"), ("p99Micros", P99, "p99")):
        ys = [p[key] / 1000 for p in pts]
        ax1.plot(xs, ys, "-", color=color, linewidth=2, marker="o", markersize=5, label=name, zorder=3)
    for p in pts:
        ax1.annotate(str(p["workers"]), (p["medianSteadyWfs"], p["p99Micros"] / 1000), textcoords="offset points",
                     xytext=(0, 7), ha="center", color=INK_2, fontsize=8)
    ax1.set_yscale("log")
    ax1.set_yticks([1, 2, 5, 10, 20, 50])
    ax1.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    ax1.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax1.set_xlabel("Throughput, workflows/s (median of 3)")
    ax1.set_ylabel("Workflow latency, ms (log)")
    ax1.set_title("Throughput vs latency; labels = concurrent workflows (= workers)", loc="left", color=INK, fontsize=11)
    ax1.legend(frameon=False, fontsize=9, labelcolor=INK_2, loc="upper left")
    labels = [str(p["workers"]) for p in pts]
    q = [p["engineCommand"]["slowest1PctQueueWaitShare"] for p in pts]
    f = [p["engineCommand"]["slowest1PctBatchFormationShare"] for p in pts]
    w = [p["engineCommand"]["slowest1PctWriteShare"] for p in pts]
    xi = range(len(pts))
    ax2.bar(xi, w, color=WRITE, width=0.62, label="durable write", edgecolor=SURFACE, linewidth=2)
    ax2.bar(xi, f, bottom=w, color=FORM, width=0.62, label="batch formation", edgecolor=SURFACE, linewidth=2)
    ax2.bar(xi, q, bottom=[a + b for a, b in zip(w, f)], color=QUEUE, width=0.62, label="queue wait", edgecolor=SURFACE, linewidth=2)
    ax2.set_xticks(list(xi), labels)
    ax2.set_xlabel("Concurrent workflows")
    ax2.yaxis.set_major_formatter(PercentFormatter(1.0))
    ax2.set_ylim(0, 1.05)
    ax2.set_title("Slowest 1% of engine commands: where the time went", loc="left", color=INK, fontsize=11)
    ax2.legend(frameon=False, fontsize=9, labelcolor=INK_2, loc="upper center", ncol=3, bbox_to_anchor=(0.5, -0.14))
    fig.suptitle("Canonical configuration: JDK 21, write-through journal, -XX:MaxGCPauseMillis=5, 3-step durable workflow, one laptop",
                 x=0.01, ha="left", color=INK, fontsize=12)
    fig.tight_layout()
    fig.savefig(args.out, facecolor=SURFACE)
    fig.savefig(str(Path(args.out).with_suffix(".png")), facecolor=SURFACE, dpi=150)
    print("wrote", args.out)


if __name__ == "__main__":
    main()
