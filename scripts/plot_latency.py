"""Plots latency vs offered load from scripts/latency_sweep.py.

    python scripts/plot_latency.py benchmarks/latency/canonical --out docs/latency.svg

Panel 1: p50, p99 and p99.9 of the default configuration against offered load (fraction of capacity
measured just before each run), log latency. Panel 2: p99 of the default (batch cap 256) against the
cap-64 comparison at the same loads. Two measures, two panels; no second y-axis.
"""
import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter, PercentFormatter  # noqa: E402

INK, INK_2, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#a3a29d", "#e6e5e1", "#fcfcfb"
P50, P99, P999 = "#2a78d6", "#eb6834", "#1baf7a"  # reference palette slots 1-3


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("folder")
    ap.add_argument("--out", default="docs/latency.svg")
    args = ap.parse_args()
    s = json.loads((Path(args.folder) / "summary.json").read_text())
    rows = {}
    for r in s["openLoop"]:
        rows.setdefault(r["config"], []).append(r)
    for v in rows.values():
        v.sort(key=lambda r: r["fractionOfCapacity"])
    plt.rcParams.update({"font.family": "sans-serif", "font.size": 10, "axes.edgecolor": GRID,
                         "axes.labelcolor": INK_2, "xtick.color": INK_2, "ytick.color": INK_2})
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 5), facecolor=SURFACE)
    for ax in (ax1, ax2):
        ax.set_facecolor(SURFACE)
        ax.grid(True, color=GRID, linewidth=1)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.set_yscale("log")
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
        ax.xaxis.set_major_formatter(PercentFormatter(1.0))
        ax.set_xlabel("Offered load (fraction of capacity measured just before each run)")
        ax.set_ylabel("Workflow latency, ms (log)")
    d = rows["default"]
    xs = [r["fractionOfCapacity"] for r in d]
    unloaded = s["unloaded"]["default"]
    for key, color, name in (("p50Ms", P50, "p50"), ("p99Ms", P99, "p99"), ("p999Ms", P999, "p99.9")):
        ys = [r[key] for r in d]
        ax1.plot(xs, ys, "-", color=color, linewidth=2, marker="o", markersize=5, label=name, zorder=3)
        nudge = {"p99.9": 8, "p99": -8}.get(name, 0)
        ax1.annotate(f"{ys[-1]:,.0f} ms" if ys[-1] >= 10 else f"{ys[-1]:.1f} ms", (xs[-1], ys[-1]),
                     textcoords="offset points", xytext=(7, nudge), va="center", color=INK, fontsize=9)
    ax1.axvspan(0.5, 0.75, color=GRID, alpha=0.5, zorder=0)
    ax1.text(0.625, ax1.get_ylim()[0] * 1.3, "knee", ha="center", color=INK_2, fontsize=9)
    ax1.set_title(f"Default config, 256 workers (unloaded p50 {unloaded['p50Ms']} ms, p99 {unloaded['p99Ms']} ms)",
                  loc="left", color=INK, fontsize=11)
    for name, color, style in (("default", P99, "-"), ("cap64", MUTED, ":")):
        v = rows[name]
        ax2.plot([r["fractionOfCapacity"] for r in v], [r["p99Ms"] for r in v], style, color=color, linewidth=2,
                 marker="o", markersize=5, label="batch cap 256 (default)" if name == "default" else "batch cap 64",
                 zorder=3 if name == "default" else 2)
    ax2.set_title("p99: does the larger batch cap cost tail latency?", loc="left", color=INK, fontsize=11)
    for ax in (ax1, ax2):
        ax.set_xticks(xs)
        ax.legend(frameon=False, fontsize=9, labelcolor=INK_2, loc="upper left")
    fig.suptitle("Latency vs load, open loop (JDK 21, write-through journal, -XX:MaxGCPauseMillis=5, one laptop)",
                 x=0.01, ha="left", color=INK, fontsize=12)
    fig.tight_layout()
    fig.savefig(args.out, facecolor=SURFACE)
    fig.savefig(str(Path(args.out).with_suffix(".png")), facecolor=SURFACE, dpi=150)
    print("wrote", args.out)


if __name__ == "__main__":
    main()
