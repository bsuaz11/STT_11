#!/usr/bin/env python
"""
Phase2/plot_equity.py — the deliverable's one plot.

Held-out backtest sessions as excess P&L over the do-nothing control. After
the live block, pass the JSON from live_compare.py to overlay the live curve:

    python Phase2/plot_equity.py [--live Phase2/results/live_week06.json]
"""

from __future__ import annotations

import argparse
import json
import pathlib

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

HERE = pathlib.Path(__file__).resolve().parent
LIVE_SEATS = ("stt_11_trader_1", "stt_11_trader_2")


def main() -> None:
    ap = argparse.ArgumentParser(description="Plot the equity curve")
    ap.add_argument("--results", default=str(HERE / "results" / "backtest_results.json"))
    ap.add_argument("--live", default=None)
    ap.add_argument("--out", default=str(HERE / "equity_curve.png"))
    args = ap.parse_args()

    data = json.loads(pathlib.Path(args.results).read_text())
    curves = list(data["curves"].values())
    n = min(len(c) for c in curves)
    grid = np.array([c[:n] for c in curves])
    ticks = np.arange(n)

    fig, ax = plt.subplots(figsize=(9, 4.8), dpi=150)
    for row in grid:
        ax.plot(ticks, row, color="#9db4c9", lw=0.6, alpha=0.5)
    ax.fill_between(ticks, *np.percentile(grid, [10, 90], axis=0), color="#3b6ea5", alpha=0.18,
                    label="10th to 90th percentile")
    ax.plot(ticks, grid.mean(axis=0), color="#1f4e79", lw=2,
            label=f"Backtest mean, {len(curves)} held-out seeds")

    if args.live:
        live = json.loads(pathlib.Path(args.live).read_text())["analysis"]
        series = [live["equity"][s] for s in LIVE_SEATS if s in live["equity"]]
        if series:
            start = min(s[0][0] for s in series)
            by_tick: dict[int, float] = {}
            for s in series:
                base = s[0][1]
                for tick, worth in s:
                    by_tick[tick] = by_tick.get(tick, 0.0) + worth - base
            t = sorted(by_tick)
            ax.plot([x - start for x in t], [by_tick[x] for x in t], color="#c0392b", lw=1.8,
                    label="Live, trader seats (not net of control)")

    ax.axhline(0, color="#555", lw=0.8)
    ax.set_xlabel("Ticks since the session opened")
    ax.set_ylabel("Excess P&L over do-nothing control ($)")
    ax.set_title("EventGap on the week 6 scenario, net of 40 bps taker fees")
    ax.legend(frameon=False, loc="upper left")
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(args.out)
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
