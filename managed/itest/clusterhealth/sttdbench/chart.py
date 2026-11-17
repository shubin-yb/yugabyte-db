#!/usr/bin/env python3
"""Charts sttd_bench.py results: latency vs tablets per phase, per-call timelines, and a table.

usage: chart.py OUT_DIR RUN_DIR [RUN_DIR...]   (needs matplotlib)
"""
import json
import os
import re
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sttd_bench import classify, iso_ms, read_samples, summarize  # noqa: E402

# Categorical slots 1-3 of the dataviz reference palette (light mode), fixed per phase.
PHASES = [("idle", "Idle", "#2a78d6"), ("saturate", "Saturated (ysql_bench)", "#eb6834"),
          ("write", "Heavy writes (16 KiB batches)", "#1baf7a")]
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False, "font.size": 10,
    "axes.titlesize": 11, "axes.titleweight": "bold", "axes.titlelocation": "left",
})


def load(run_dirs):
    runs = []
    for d in run_dirs:
        s = summarize(d)
        s["dir"] = d
        runs.append(s)
    runs.sort(key=lambda r: r["meta"]["user_tablets_per_tserver"])
    return runs


def fmt_ms(v):
    if v is None:
        return "-"
    return "%.0f ms" % v if v < 10000 else "%.1f s" % (v / 1000)


MS_TICKS = [1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000]


def ms_tick_label(v):
    return ("%d ms" % v) if v < 1000 else ("%d s" % (v // 1000))


def not_safe_pct(p):
    return 100.0 * (p["unsafe"] + p["error"]) / p["samples"] if p["samples"] else None


def end_labels(ax, items):
    """Direct labels right of the line ends, nudged apart in display space (works on log axes)."""
    if not items:
        return
    fwd, inv = ax.transData.transform, ax.transData.inverted().transform
    pts = sorted((fwd((x, y))[1], x, y, text) for x, y, text in items)
    min_gap = 16  # display units at the canvas dpi
    placed = []
    for dy, x, y, text in pts:
        if placed and dy - placed[-1][0] < min_gap:
            dy = placed[-1][0] + min_gap
        placed.append((dy, x, y, text))
    for dy, x, y, text in placed:
        tx, ty = inv((fwd((x, y))[0] + 12, dy))
        ax.text(tx, ty, text, va="center", ha="left", fontsize=9, color=INK2)


def latency_vs_tablets(runs, path):
    panels = [
        ("Master handler mean (server side, excludes client overhead)",
         lambda p: p["master_handler_mean_ms"], "log"),
        ("p99 wall time per call (yb-admin, includes ~80 ms start-up)",
         lambda p: p["wall_ms"]["p99"], "log"),
        ("p50 wall time per call", lambda p: p["wall_ms"]["p50"], "log"),
        ("Calls not reported safe (unsafe or error)", not_safe_pct, "pct"),
    ]
    fig, axes = plt.subplots(2, 2, figsize=(14, 9.5))
    xs = [r["meta"]["user_tablets_per_tserver"] for r in runs]
    for ax, (title, get, kind) in zip(axes.flat, panels):
        if kind == "log":
            ax.set_yscale("log")
            ax.set_ylim(1, 60000)
            ax.set_yticks(MS_TICKS)
            ax.set_yticklabels([ms_tick_label(v) for v in MS_TICKS])
            ax.minorticks_off()
            ax.axhline(28000, color=INK2, linewidth=1, linestyle=":", zorder=1)
            ax.text(xs[0], 33000, "28 s: client deadline minus 2 s", fontsize=8, color=INK2)
        else:
            ax.set_ylim(0, 105)
            ax.set_yticks([0, 25, 50, 75, 100])
            ax.set_yticklabels(["0%", "25%", "50%", "75%", "100%"])
        ax.set_xlim(0, max(xs) * 1.22)
        ax.set_xticks(xs)
        ax.set_xlabel("User tablets per tserver")
        ax.set_title(title)
        labels = []
        for key, label, color in PHASES:
            pts = [(x, get(r["phases"][key])) for x, r in zip(xs, runs)
                   if key in r["phases"] and get(r["phases"][key]) is not None]
            if not pts:
                continue
            px, py = zip(*pts)
            if kind == "log":
                py = [max(v, 1) for v in py]
            ax.plot(px, py, color=color, linewidth=2, marker="o", markersize=8,
                    markeredgecolor=SURFACE, markeredgewidth=2, label=label, zorder=3)
            labels.append((px[-1], py[-1], fmt_ms(py[-1]) if kind == "log" else "%.0f%%" % py[-1]))
        fig.canvas.draw()
        end_labels(ax, labels)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 0.995))
    fig.suptitle("AreNodesSafeToTakeDown cost vs tablet count: one tserver per call, RF3, 3 x "
                 "8 vCPU / 32 GiB tservers on pd-standard, 60 s follower lag bound",
                 y=1.03, fontsize=12, fontweight="bold", x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def timelines(runs, path):
    fig, axes = plt.subplots(len(runs), 1, figsize=(15, 3.4 * len(runs)), squeeze=False)
    colors = {k: c for k, _, c in PHASES}
    for ax, r in zip(axes[:, 0], runs):
        meta = r["meta"]
        # Phases are laid end to end with a 1 minute gap, each from its own start: a merged run's
        # phases come from different passes, so wall-clock offsets would mean nothing.
        offsets, x = {}, 0.0
        for ph in meta["phases"]:
            offsets[ph["name"]] = x
            x += (ph["end_ms"] - ph["start_ms"]) / 60000 + 1

        def xpos(ph, t_ms):
            return offsets[ph["name"]] + (t_ms - ph["start_ms"]) / 60000

        samples = [s for s in read_samples(r["dir"]) if s["kind"] == "sttd"
                   and any(p["start_ms"] <= s["t_ms"] <= p["end_ms"] for p in meta["phases"])]
        # Log scale: calls range from ~80 ms (mostly yb-admin start-up) to the 28 s deadline.
        ax.set_yscale("log")
        ymax = 40000
        ticks = [50, 100, 200, 500, 1000, 2000, 5000, 10000, 30000]
        ax.set_yticks(ticks)
        ax.set_yticklabels([("%d ms" % v) if v < 1000 else ("%d s" % (v // 1000)) for v in ticks])
        ax.minorticks_off()
        for ph in meta["phases"]:
            a, b = xpos(ph, ph["start_ms"]), xpos(ph, ph["end_ms"])
            ax.axvspan(a, b, color=colors[ph["name"]], alpha=0.07, linewidth=0)
            ax.text(a + 0.1, ymax * 0.8, ph["name"], color=INK2, fontsize=9, va="top")
            pts = [s for s in samples if ph["start_ms"] <= s["t_ms"] <= ph["end_ms"]]
            ok = [s for s in pts if classify(s) == "safe"]
            bad = [s for s in pts if classify(s) != "safe"]
            ax.scatter([xpos(ph, s["t_ms"]) for s in ok], [s["wall_ms"] for s in ok],
                       s=14, color=colors[ph["name"]], linewidths=0, zorder=3)
            if bad:
                ax.scatter([xpos(ph, s["t_ms"]) for s in bad], [s["wall_ms"] for s in bad],
                           s=40, marker="x", color=INK, linewidths=1.5, zorder=4,
                           label="unsafe / error")
            for i, d in enumerate(ph.get("disruptions", [])):
                ts, what = d.split(" ", 1)
                if ts == "?":
                    continue
                x = xpos(ph, iso_ms(ts))
                ax.axvline(x, color=INK2, linewidth=1, linestyle="--", zorder=2)
                # "yb<universe>-<suffix>-yb-tserver-1 ..." -> "tserver-1 ..."
                short = re.sub(r"^\S*-yb-(master|tserver)", r"\1", what).split(" (")[0]
                ax.text(x + 0.05, ymax * 0.35 / (2 ** i), short, fontsize=8, color=INK2)
        ax.set_ylim(40, ymax)
        merged = ""
        if meta.get("merged_from"):
            merged = "; phases from separate passes: " + ", ".join(
                "%s (%s)" % (p["name"], p.get("source", "?")) for p in meta["phases"])
        ax.set_title("%d user tablets per tserver: every call (dots = safe, x = unsafe/error, "
                     "dashed = pod restart or eviction)%s" % (meta["user_tablets_per_tserver"], merged))
        ax.set_ylabel("wall time per call")
        ax.set_xlabel("minutes into each phase (phases laid end to end)")
    fig.tight_layout()
    fig.savefig(path, dpi=130, bbox_inches="tight")
    plt.close(fig)


def table(runs, path):
    cols = ["tablets/tserver", "phase", "clean window", "calls", "safe", "unsafe", "error", "p50",
            "p90", "p99", "max", "yb-admin overhead p50", "master handler mean",
            "tserver health RPC mean", "tserver RPC queue mean", "max follower lag",
            "tserver CPU avg / max (m)", "YBA sweeps (ms)", "load",
            "after first disruption: calls / unsafe / error / max", "disruptions"]
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in runs:
        for key, _, _ in PHASES:
            p = r["phases"].get(key)
            if not p:
                continue
            w = p["wall_ms"]
            tail = p["disrupted_tail"]
            lines.append("| " + " | ".join([
                str(r["meta"]["user_tablets_per_tserver"]), key, "%ds" % p["clean_seconds"],
                str(p["samples"]), str(p["safe"]),
                str(p["unsafe"]), str(p["error"]), fmt_ms(w["p50"]), fmt_ms(w["p90"]),
                fmt_ms(w["p99"]), fmt_ms(w["max"]), fmt_ms(p["yb_admin_overhead_ms_p50"]),
                fmt_ms(p["master_handler_mean_ms"]), fmt_ms(p["tserver_health_rpc_mean_ms"]),
                fmt_ms(p["tserver_rpc_queue_mean_ms"]),
                fmt_ms(p["max_follower_lag_ms"]),
                "%s / %s" % (int(p["tserver_cpu_m_avg"] or 0), int(p["tserver_cpu_m_max"] or 0)),
                ", ".join(str(v) for v in p["yba_sweep_ms"][:6]) or "-",
                p.get("load_throughput", "-"),
                "%d / %d / %d / %s" % (tail["samples"], tail["unsafe"], tail["error"],
                                       fmt_ms(tail["max_wall_ms"])) if tail["samples"] else "-",
                "; ".join(p["disruptions"]) or "-",
            ]) + " |")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


def main():
    out, run_dirs = sys.argv[1], sys.argv[2:]
    os.makedirs(out, exist_ok=True)
    runs = load(run_dirs)
    latency_vs_tablets(runs, os.path.join(out, "sttd_latency_vs_tablets.png"))
    timelines(runs, os.path.join(out, "sttd_timelines.png"))
    table(runs, os.path.join(out, "sttd_summary.md"))
    print("wrote", ", ".join(os.path.join(out, f) for f in
                             ("sttd_latency_vs_tablets.png", "sttd_timelines.png", "sttd_summary.md")))


if __name__ == "__main__":
    main()
