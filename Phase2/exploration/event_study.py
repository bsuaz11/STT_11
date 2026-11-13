#!/usr/bin/env python
"""
Phase2/exploration/event_study.py — the real book, tick by tick, around each print.

The exploration that shaped the EventGap hypothesis: after a CALENDAR_EVENT,
how far do the mid, the touch and the venue mark move from the pre-print mid,
and how much size is left on each side? Also prints the tape's fee schedule.

    python Phase2/exploration/event_study.py <tape.jsonl.gz> ...
"""

from __future__ import annotations

import gzip
import json
import sys
from collections import defaultdict

import numpy as np

OFFSETS = (-1, 0, 1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 18, 20, 23, 25, 30, 40, 60)


def load(path: str) -> tuple[dict, list[tuple], list[tuple]]:
    """Book rows per symbol and tick, the prints, and the fills' fees."""
    books: dict[str, dict[int, tuple]] = defaultdict(dict)
    prints: list[tuple] = []
    fills: list[tuple] = []
    with gzip.open(path, "rt") as f:
        for line in f:
            if ('"book_snapshot"' not in line and '"session_event"' not in line
                    and '"trade_execution"' not in line):
                continue
            rec = json.loads(line)
            msg, kind = rec["msg"], rec["msg"].get("type")
            if kind == "book_snapshot":
                bids, asks = msg["bids"], msg["asks"]
                books[msg["symbol"]][rec["tick"]] = (
                    bids[0][0] if bids else None, asks[0][0] if asks else None,
                    msg["mid_price"], msg.get("ref_price"),
                    sum(level[1] for level in bids), sum(level[1] for level in asks))
            elif kind == "session_event" and msg["event"] == "CALENDAR_EVENT":
                d = msg["data"]
                prints.append((rec["tick"], d.get("kind"), d["pct"], d.get("ramp_ticks"),
                               d.get("overshoot"), d.get("affected")))
            elif kind == "trade_execution":
                fills.append((msg["quantity"], msg["price"], msg.get("fee"), msg.get("maker_rebate")))
    return books, prints, fills


def main() -> None:
    """Run over the tape(s) named on the command line."""
    for path in sys.argv[1:]:
        books, prints, fills = load(path)
        charged = [f for f in fills if f[2]]
        print(path, "fee/notional", np.median([f[2] / (f[0] * f[1]) for f in charged]),
              "rebate/notional", np.median([(f[3] or 0) / (f[0] * f[1]) for f in charged]))
        for tick, *info, affected in prints:
            print("EVENT", (tick, *info), "n_affected", len(affected))
            sym = affected[0]                     # the first affected symbol stands for all
            if sym == "ARENA10":
                continue
            rows = books[sym]
            base = None
            for back in range(-3, 0):
                if tick + back in rows and rows[tick + back][2]:
                    base = rows[tick + back][2]
            print(" ", sym, "base", base, "  rel: mid% bid% ask% ref% bidsz asksz")

            def rel(price: float | None, base: float = base) -> str:
                return f"{(price / base - 1) * 100:+6.2f}" if price else "  None"
            for offset in OFFSETS:
                row = rows.get(tick + offset)
                if row:
                    print(f"   t{offset:+3d} {rel(row[2])} {rel(row[0])} {rel(row[1])} "
                          f"{rel(row[3])} {row[4]:6.0f} {row[5]:6.0f}")


if __name__ == "__main__":
    main()
