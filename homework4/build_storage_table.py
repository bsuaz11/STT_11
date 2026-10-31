#!/usr/bin/env python3
"""storage_report.py — the storage table: one row per week, measured.

    python storage_report.py tapes/session_week*.jsonl.gz [--scratch /tmp/storage_report]
"""
from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from load_into_parquet import write_week
from load_into_sqlite import INDEXES, SCHEMA, load_week 
from algo_recordings import COLUMNS, guess_week, jsonl_bytes


def main() -> int:
    ap = argparse.ArgumentParser(description = __doc__.split("\n")[0])
    ap.add_argument("tapes", nargs = "+")
    ap.add_argument("--scratch", default = "/tmp/storage_report")
    a = ap.parse_args()
    shutil.rmtree(a.scratch, ignore_errors = True)
    os.makedirs(a.scratch)
    out = []
    for p in a.tapes:
        wk = guess_week(p)
        db = os.path.join(a.scratch, f"w{wk}.sqlite")
        con = sqlite3.connect(db)
        con.executescript(SCHEMA)
        r = load_week(con, p, wk)
        noidx = os.path.getsize(db)
        t = time.perf_counter()
        con.executescript(INDEXES)
        con.commit()
        t_idx = time.perf_counter() - t
        idx = os.path.getsize(db)
        con.close()
        wh = os.path.join(a.scratch, "pq")
        q = write_week(p, wk, wh, "zstd", 4096)
        out.append(dict(week = wk, tape = os.path.basename(p), jsonl = jsonl_bytes(p),
                        gz = os.path.getsize(p), noidx = noidx, idx = idx, t_idx = t_idx,
                        pq = sum(q["bytes"].values()), t_sq = r["load_s"], t_pq = q["total_s"],
                        rows = r["rows"], pq_bytes = q["bytes"]))

    def mb(b):
        return f"{b / 1e6:,.1f} MB"

    print("| week | JSONL in (gz on disk) | SQLite, no index | SQLite, 5 indexes | Parquet (zstd) "
          "| SQLite load | index build | Parquet convert |")
    print("|---:|---:|---:|---:|---:|---:|---:|---:|")
    for o in out:
        print(f"| {o['week']} | {mb(o['jsonl'])} ({mb(o['gz'])}) | {mb(o['noidx'])} | "
              f"{mb(o['idx'])} | {mb(o['pq'])} | {o['t_sq']:.1f} s | {o['t_idx']:.2f} s | "
              f"{o['t_pq']:.1f} s |")
    print("\nrows per table (Parquet bytes in brackets; book_level exists only in SQLite — in "
          "Parquet the depth is list columns on book_snapshot)")
    print("| table | " + " | ".join(f"week {o['week']}" for o in out) + " |")
    print("|---|" + "---:|" * len(out))
    for k in COLUMNS:
        cells = []
        for o in out:
            b = o["pq_bytes"].get(k)
            cells.append(f"{o['rows'][k]:,}" + (f" ({mb(b)})" if b else ""))
        print(f"| {k} | " + " | ".join(cells) + " |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
