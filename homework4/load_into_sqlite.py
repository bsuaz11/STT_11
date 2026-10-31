#!/usr/bin/env python3
"""load_sqlite.py — add one or more weeks of recordings to the SQLite store.

    python load_sqlite.py tapes/session_week02_*.jsonl.gz [more tapes ...] --db stores/arena.sqlite
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from algo_recordings import COLUMNS, guess_week, jsonl_bytes, read_tape  # noqa: E402

SCHEMA = """
CREATE TABLE IF NOT EXISTS book_snapshot (
    week INTEGER NOT NULL, seq INTEGER NOT NULL,
    tick INTEGER NOT NULL, t INTEGER NOT NULL, ts REAL NOT NULL,
    symbol TEXT NOT NULL,
    best_bid REAL, best_bid_qty REAL, best_ask REAL, best_ask_qty REAL,
    mid REAL, spread REAL, microprice REAL, obi REAL, ref_price REAL,
    n_bid INTEGER NOT NULL, n_ask INTEGER NOT NULL,
    PRIMARY KEY (week, seq)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS book_level (
    week INTEGER NOT NULL, seq INTEGER NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('B', 'A')), lvl INTEGER NOT NULL,
    px REAL NOT NULL, qty REAL NOT NULL,
    PRIMARY KEY (week, seq, side, lvl),
    FOREIGN KEY (week, seq) REFERENCES book_snapshot (week, seq)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS trade_execution (
    week INTEGER NOT NULL, seq INTEGER NOT NULL,
    tick INTEGER NOT NULL, t INTEGER NOT NULL, ts REAL NOT NULL,
    symbol TEXT NOT NULL, trade_id TEXT, price REAL NOT NULL, quantity INTEGER NOT NULL,
    notional REAL NOT NULL, aggressor TEXT CHECK (aggressor IN ('buy', 'sell')),
    buyer_id TEXT, seller_id TEXT, taker_id TEXT, maker_id TEXT,
    fee REAL, maker_rebate REAL,
    PRIMARY KEY (week, seq)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS order_ack (
    week INTEGER NOT NULL, seq INTEGER NOT NULL,
    tick INTEGER NOT NULL, t INTEGER NOT NULL, ts REAL NOT NULL,
    symbol TEXT, order_id TEXT, team_id TEXT, side TEXT, price REAL, quantity INTEGER,
    queue_ahead INTEGER, level_qty INTEGER,
    PRIMARY KEY (week, seq)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS order_cancel (
    week INTEGER NOT NULL, seq INTEGER NOT NULL,
    tick INTEGER NOT NULL, t INTEGER NOT NULL, ts REAL NOT NULL,
    symbol TEXT, order_id TEXT, team_id TEXT, fee REAL,
    PRIMARY KEY (week, seq)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS order_reject (
    week INTEGER NOT NULL, seq INTEGER NOT NULL,
    tick INTEGER NOT NULL, t INTEGER NOT NULL, ts REAL NOT NULL,
    team_id TEXT, code TEXT, message TEXT,
    PRIMARY KEY (week, seq)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS session_event (
    week INTEGER NOT NULL, seq INTEGER NOT NULL,
    tick INTEGER NOT NULL, t INTEGER NOT NULL, ts REAL NOT NULL,
    event TEXT NOT NULL, message TEXT, kind TEXT, symbol TEXT, pct REAL,
    ramp_ticks INTEGER, overshoot REAL, affected TEXT, data TEXT,
    PRIMARY KEY (week, seq)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS leaderboard (
    week INTEGER NOT NULL, seq INTEGER NOT NULL,
    tick INTEGER NOT NULL, t INTEGER NOT NULL, ts REAL NOT NULL,
    team_id TEXT NOT NULL, role TEXT, cash REAL, net_worth REAL,
    realized_pnl REAL, unrealized_pnl REAL, fees_paid REAL, rebates REAL, trade_count INTEGER,
    PRIMARY KEY (week, seq, team_id)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS load_log (
    week INTEGER PRIMARY KEY, tape TEXT NOT NULL, jsonl_bytes INTEGER, t0_tick INTEGER,
    load_s REAL, loaded_at TEXT DEFAULT CURRENT_TIMESTAMP);
"""
INDEXES = """
CREATE INDEX IF NOT EXISTS book_wst    ON book_snapshot (week, symbol, t, seq);
CREATE INDEX IF NOT EXISTS trade_taker ON trade_execution (taker_id, week, seq);
CREATE INDEX IF NOT EXISTS trade_maker ON trade_execution (maker_id, week, seq);
CREATE INDEX IF NOT EXISTS ack_team    ON order_ack (team_id, week, seq);
CREATE INDEX IF NOT EXISTS board_team  ON leaderboard (team_id, week, t);
"""
TABLES = [k for k in COLUMNS]


def load_week(con: sqlite3.Connection, path: str, week: int) -> dict:
    if con.execute("SELECT 1 FROM load_log WHERE week = ?", (week,)).fetchone():
        raise SystemExit(f"week {week} is already loaded: the store is append-only "
                         f"(delete the .sqlite and rebuild if you really mean it)")
    t_start = time.perf_counter()
    cols, info = read_tape(path, week)
    t_parse = time.perf_counter() - t_start
    rows = {}
    with con:                                           
        for table in TABLES:
            names = COLUMNS[table]
            data = list(zip(*(cols[table][c] for c in names)))
            con.executemany(f"INSERT INTO {table} ({','.join(names)}) VALUES "
                            f"({','.join('?' * len(names))})", data)
            rows[table] = len(data)
        load_s = time.perf_counter() - t_start
        con.execute("INSERT INTO load_log (week, tape, jsonl_bytes, t0_tick, load_s) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (week, os.path.basename(path), jsonl_bytes(path), info["t0"], load_s))
    return {"rows": rows, "parse_s": t_parse, "load_s": time.perf_counter() - t_start, **info}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("tapes", nargs="+")
    ap.add_argument("--db", default="stores/arena.sqlite")
    ap.add_argument("--week", type=int, default=None, help="override (one tape only)")
    ap.add_argument("--no-index", action="store_true", help="skip CREATE INDEX (for measuring)")
    a = ap.parse_args()
    if a.week is not None and len(a.tapes) > 1:
        raise SystemExit("--week only with a single tape")
    os.makedirs(os.path.dirname(os.path.abspath(a.db)) or ".", exist_ok=True)
    con = sqlite3.connect(a.db)
    con.executescript(SCHEMA)
    for p in a.tapes:
        wk = a.week if a.week is not None else guess_week(p)
        r = load_week(con, p, wk)
        print(f"week {wk}: {os.path.basename(p)} · t0 = tick {r['t0']} ({r['t0_from']}) · "
              f"parse {r['parse_s']:.2f} s · parse+insert {r['load_s']:.2f} s")
        for k, n in r["rows"].items():
            print(f"   {k:<16} {n:>9,}")
    size0 = os.path.getsize(a.db)
    if not a.no_index:
        t = time.perf_counter()
        con.executescript(INDEXES)
        con.commit()
        print(f"indexes: {time.perf_counter() - t:.2f} s · file {size0:,} -> "
              f"{os.path.getsize(a.db):,} bytes")
    weeks = [w for (w,) in con.execute("SELECT week FROM load_log ORDER BY week")]
    print(f"{a.db}: weeks {weeks}")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
