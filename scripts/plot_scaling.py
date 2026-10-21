"""Plots a scripts/scaling_sweep.py summary: throughput and efficiency vs worker count.

    python scripts/plot_scaling.py benchmarks/scaling/ab --highlight pipelined --out docs/scaling.svg

Panel 1: steady-state throughput (log-log), highlighted config solid, others muted and dotted, and
the ideal-linear extrapolation from 1 worker dashed. Hollow markers: the pre-run fsync probe said the
disk was in its slow state (median > 1.5 ms), so that point is not comparable with filled ones.
Panel 2: scaling efficiency of the highlighted config for activities with latency (the worker-pool
question), throughput / (workers x 1-worker throughput). Two measures, two panels: never a second y-axis.
"""
import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter, PercentFormatter  # noqa: E402

INK, INK_2, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#a3a29d", "#e6e5e1", "#fcfcfb"
SERIES = {0: "#2a78d6", 10: "#eb6834"}  # reference palette slots 1-2, keyed by activity latency
NAMES = {0: "instant activities", 10: "10 ms activities"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("folder")
    ap.add_argument("--highlight", default="pipelined")
    ap.add_argument("--out", default="docs/scaling.svg")
    ap.add_argument("--title", default="Journal engine scaling with worker count — one laptop, every transition fsynced")
    args = ap.parse_args()
    data = json.loads((Path(args.folder) / "summary.json").read_text())
    series, slow = {}, {}
    for p in data["points"]:
        series.setdefault((p["config"], p["activityMs"]), []).append((p["workers"], p["medianSteadyWfs"]))
        probes = sorted(x for x in p.get("diskProbeMs", []) if x is not None)
        slow[(p["config"], p["activityMs"], p["workers"])] = bool(probes) and probes[len(probes) // 2] > 1.5
    plt.rcParams.update({"font.family": "sans-serif", "font.size": 10, "axes.edgecolor": GRID,
                         "axes.labelcolor": INK_2, "xtick.color": INK_2, "ytick.color": INK_2})
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 5), facecolor=SURFACE)
    for ax in (ax1, ax2):
        ax.set_facecolor(SURFACE)
        ax.set_xscale("log", base=2)
        ax.grid(True, color=GRID, linewidth=1)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.set_xlabel("Workers")
    workers_ticks = sorted({w for rows in series.values() for w, _ in rows})
    for (config, activity), rows in sorted(series.items(), key=lambda kv: kv[0][0] == args.highlight):
        rows.sort()
        xs, ys = [r[0] for r in rows], [r[1] for r in rows]
        hot = config == args.highlight
        color = SERIES.get(activity, INK_2) if hot else MUTED
        label = f"{NAMES.get(activity, f'{activity} ms')}, {config}"
        ax1.plot(xs, ys, "-" if hot else ":", color=color, linewidth=2, label=label, zorder=3 if hot else 2)
        for x, y in zip(xs, ys):
            hollow = slow.get((config, activity, x))
            ax1.plot([x], [y], "o", markersize=6 if hot else 4, color=color, zorder=4 if hot else 2,
                     markerfacecolor=SURFACE if hollow else color, markeredgewidth=1.5)
        if hot:
            ax1.plot(xs, [ys[0] * x / xs[0] for x in xs], "--", color=color, linewidth=1, alpha=0.4, zorder=1)
            ax1.annotate(f"{ys[-1]:,.0f}/s", (xs[-1], ys[-1]), textcoords="offset points", xytext=(7, 0),
                         va="center", color=INK, fontsize=9)
            if activity == 0:
                continue
            eff = [y / (x / xs[0] * ys[0]) for x, y in zip(xs, ys)]
            ax2.plot(xs, eff, "-", color=color, linewidth=2, marker="o", markersize=5, label=NAMES.get(activity), zorder=3)
            ax2.annotate(f"{eff[-1]:.0%}", (xs[-1], eff[-1]), textcoords="offset points", xytext=(7, 0),
                         va="center", color=INK, fontsize=9)
    ax1.set_yscale("log")
    ax1.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax1.set_ylabel("Workflows / s, steady state (log)")
    ax1.set_title("Throughput (dashed: linear from 1 worker; hollow: slow-disk run)", loc="left", color=INK, fontsize=11)
    ax2.set_ylim(0, 1.2)
    ax2.yaxis.set_major_formatter(PercentFormatter(1.0))
    ax2.set_ylabel("Scaling efficiency vs 1 worker")
    ax2.set_title(f"Worker-pool efficiency, 10 ms activities, {args.highlight}", loc="left", color=INK, fontsize=11)
    for ax in (ax1, ax2):
        ax.set_xticks(workers_ticks)
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
        ax.legend(frameon=False, fontsize=8.5, labelcolor=INK_2, loc="upper left")
    fig.suptitle(args.title, x=0.01, ha="left", color=INK, fontsize=12)
    fig.tight_layout()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out, facecolor=SURFACE)
    fig.savefig(str(Path(args.out).with_suffix(".png")), facecolor=SURFACE, dpi=150)
    print("wrote", args.out)


if __name__ == "__main__":
    main()
