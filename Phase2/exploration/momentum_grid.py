#!/usr/bin/env python
"""
Phase2/exploration/momentum_grid.py — does a big k-tick move continue, after costs?

Exploration on the Week 2 tape only, done before the EventGap hypothesis. For
every lookback k, threshold and holding period: when the mid moves at least
the threshold over k ticks, take the move at the next tick's touch, hold h
ticks, exit at the touch and pay the taker fee both ways. Results split into
trades near a print and trades in quiet markets.

    python Phase2/exploration/momentum_grid.py homework4/session_week02_*.jsonl.gz
"""

from __future__ import annotations

import gzip
import json
import sys
from collections import defaultdict

import numpy as np

FEE_BPS = 30                      # the Week 2 tape's taker fee
LOOKBACKS = (5, 10, 20, 30, 60)
THRESHOLDS_BPS = (40, 60, 80, 120, 180)
HOLDS = (10, 20, 30, 60, 120)
EVENT_WINDOW = 60                 # a trade within this many ticks of a print is "event"
MIN_TRADES = 5


def load(path: str) -> tuple[dict[str, dict[int, tuple]], list[int]]:
    """Per symbol, tick -> (best bid, best ask, mid); and the print ticks."""
    books: dict[str, dict[int, tuple]] = defaultdict(dict)
    prints: list[int] = []
    with gzip.open(path, "rt") as f:
        for line in f:
            if '"book_snapshot"' not in line and '"session_event"' not in line:
                continue
            rec = json.loads(line)
            msg = rec["msg"]
            if msg.get("type") == "book_snapshot":
                bids, asks = msg["bids"], msg["asks"]
                books[msg["symbol"]][rec["tick"]] = (bids[0][0] if bids else np.nan,
                                                     asks[0][0] if asks else np.nan,
                                                     msg["mid_price"])
            elif msg["event"] == "CALENDAR_EVENT":
                prints.append(rec["tick"])
    return books, prints


def as_array(snaps: dict[int, tuple], first: int, last: int) -> np.ndarray:
    """One row per tick from `first` to `last`; ticks with no snapshot are NaN."""
    out = np.full((last - first + 1, 3), np.nan)
    for tick, row in snaps.items():
        out[tick - first] = row
    return out


def trades(books: dict, prints: list[int], first: int, last: int, k: int, threshold: int,
           hold: int) -> tuple[list[float], list[bool]]:
    """Net P&L in bps of every momentum trade, and whether it was near a print."""
    pnl: list[float] = []
    near_print: list[bool] = []
    for sym, snaps in books.items():
        if sym == "ARENA10":
            continue
        rows = as_array(snaps, first, last)
        bid, ask, mid = rows[:, 0], rows[:, 1], rows[:, 2]
        i, n = k, len(mid)
        while i < n - hold - 2:
            if not (mid[i] > 0 and mid[i - k] > 0):
                i += 1
                continue
            move = (mid[i] / mid[i - k] - 1) * 1e4
            if abs(move) < threshold:
                i += 1
                continue
            if move > 0:
                gross = (bid[i + 1 + hold] / ask[i + 1] - 1) * 1e4
            else:
                gross = (bid[i + 1] / ask[i + 1 + hold] - 1) * 1e4
            if np.isfinite(gross):
                pnl.append(gross - 2 * FEE_BPS)
                near_print.append(any(0 <= i + first - t <= EVENT_WINDOW for t in prints))
            i += hold + 1
    return pnl, near_print


def main() -> None:
    """Run over the tape(s) named on the command line."""
    books, prints = load(sys.argv[1])
    ticks = sorted({t for snaps in books.values() for t in snaps})
    first, last = ticks[0], ticks[-1]
    print("events", [t - first for t in prints])
    for k in LOOKBACKS:
        for threshold in THRESHOLDS_BPS:
            for hold in HOLDS:
                pnl, near = trades(books, prints, first, last, k, threshold, hold)
                if len(pnl) < MIN_TRADES:
                    continue
                p, e = np.array(pnl), np.array(near)
                t_stat = p.mean() / (p.std(ddof=1) / len(p) ** 0.5)
                print(f"k={k:3d} th={threshold:4d} h={hold:4d} n={len(p):4d} net={p.mean():+7.1f} "
                      f"hit={np.mean(p > 0):.2f} t={t_stat:+5.1f} | "
                      f"event n={e.sum():3d} net={p[e].mean() if e.any() else 0:+7.1f} | "
                      f"quiet n={(~e).sum():3d} net={p[~e].mean() if (~e).any() else 0:+7.1f}")


if __name__ == "__main__":
    main()
