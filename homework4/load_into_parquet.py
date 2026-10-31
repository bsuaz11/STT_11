#!/usr/bin/env python3
"""load_parquet.py — add one or more weeks of recordings to the Parquet store.

    python load_parquet.py tapes/session_week02_*.jsonl.gz [more tapes ...] --out stores/arena.parquet
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
import time

import pyarrow as pa
import pyarrow.dataset as ds

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from algo_recordings import guess_week, jsonl_bytes, read_tape  # noqa: E402

LIST_F = pa.list_(pa.float64())
COMMON = [("week", pa.int16()), ("seq", pa.int64()), ("tick", pa.int64()), ("t", pa.int32()),
          ("ts", pa.float64())]
SCHEMAS: dict[str, pa.Schema] = {
    "book_snapshot": pa.schema(COMMON + [
        ("symbol", pa.string()), ("best_bid", pa.float64()), ("best_bid_qty", pa.float64()),
        ("best_ask", pa.float64()), ("best_ask_qty", pa.float64()), ("mid", pa.float64()),
        ("spread", pa.float64()), ("microprice", pa.float64()), ("obi", pa.float64()),
        ("ref_price", pa.float64()), ("n_bid", pa.int16()), ("n_ask", pa.int16()),
        ("bid_px", LIST_F), ("bid_qty", LIST_F), ("ask_px", LIST_F), ("ask_qty", LIST_F)]),
    "trade_execution": pa.schema(COMMON + [
        ("symbol", pa.string()), ("trade_id", pa.string()), ("price", pa.float64()),
        ("quantity", pa.int32()), ("notional", pa.float64()), ("aggressor", pa.string()),
        ("buyer_id", pa.string()), ("seller_id", pa.string()), ("taker_id", pa.string()),
        ("maker_id", pa.string()), ("fee", pa.float64()), ("maker_rebate", pa.float64())]),
    "order_ack": pa.schema(COMMON + [
        ("symbol", pa.string()), ("order_id", pa.string()), ("team_id", pa.string()),
        ("side", pa.string()), ("price", pa.float64()), ("quantity", pa.int32()),
        ("queue_ahead", pa.int32()), ("level_qty", pa.int32())]),
    "order_cancel": pa.schema(COMMON + [
        ("symbol", pa.string()), ("order_id", pa.string()), ("team_id", pa.string()),
        ("fee", pa.float64())]),
    "order_reject": pa.schema(COMMON + [
        ("team_id", pa.string()), ("code", pa.string()), ("message", pa.string())]),
    "session_event": pa.schema(COMMON + [
        ("event", pa.string()), ("message", pa.string()), ("kind", pa.string()),
        ("symbol", pa.string()), ("pct", pa.float64()), ("ramp_ticks", pa.int32()),
        ("overshoot", pa.float64()), ("affected", pa.string()), ("data", pa.string())]),
    "leaderboard": pa.schema(COMMON + [
        ("team_id", pa.string()), ("role", pa.string()), ("cash", pa.float64()),
        ("net_worth", pa.float64()), ("realized_pnl", pa.float64()),
        ("unrealized_pnl", pa.float64()), ("fees_paid", pa.float64()),
        ("rebates", pa.float64()), ("trade_count", pa.int32())]),
}
PARTITION = {"book_snapshot": ["week", "symbol"], "trade_execution": ["week", "symbol"],
             "order_ack": ["week", "symbol"], "order_cancel": ["week", "symbol"],
             "order_reject": ["week"], "session_event": ["week"], "leaderboard": ["week"]}


def dir_bytes(d: str) -> int:
    return sum(os.path.getsize(os.path.join(dp, f)) for dp, _, fs in os.walk(d) for f in fs)


def write_week(path: str, week: int, out: str, compression: str, row_group: int) -> dict:
    existing = glob.glob(os.path.join(out, "*", f"week={week}"))
    if existing:
        raise SystemExit(f"week {week} already in {out} ({existing[0]}): the store is "
                         f"append-only; delete that week's folders to rebuild it")
    t_start = time.perf_counter()
    cols, info = read_tape(path, week)
    t_parse = time.perf_counter() - t_start
    rows, sizes = {}, {}
    for kind, schema in SCHEMAS.items():
        data = cols[kind]
        table = pa.Table.from_arrays([pa.array(data[f.name], f.type) for f in schema],
                                     schema = schema)
        table = table.sort_by([("tick", "ascending"), ("seq", "ascending")])
        rows[kind] = table.num_rows
        if table.num_rows == 0:
            continue
        ds.write_dataset(
            table, os.path.join(out, kind), format = "parquet",
            partitioning=ds.partitioning(pa.schema([schema.field(n) for n in PARTITION[kind]]),
                                         flavor = "hive"),
            basename_template = f"w{week}-{{i}}.parquet",
            file_options = ds.ParquetFileFormat().make_write_options(compression = compression),
            max_rows_per_group = row_group, min_rows_per_group = min(row_group, 1024),
            existing_data_behavior = "overwrite_or_ignore")  
        sizes[kind] = dir_bytes(os.path.join(out, kind, f"week = {week}"))
    return {"rows": rows, "bytes": sizes, "parse_s": t_parse,
            "total_s": time.perf_counter() - t_start, **info}


def main() -> int:
    ap = argparse.ArgumentParser(description = __doc__.split("\n")[0])
    ap.add_argument("tapes", nargs = "+")
    ap.add_argument("--out", default = "stores/arena.parquet")
    ap.add_argument("--week", type = int, default = None, help = "override (one tape only)")
    ap.add_argument("--compression", default="zstd", choices = ["zstd", "snappy", "gzip", "none"])
    ap.add_argument("--row-group", type = int, default = 4096, help = "max rows per row group")
    a = ap.parse_args()
    if a.week is not None and len(a.tapes) > 1:
        raise SystemExit("--week only with a single tape")
    for p in a.tapes:
        wk = a.week if a.week is not None else guess_week(p)
        r = write_week(p, wk, a.out, a.compression, a.row_group)
        tot = sum(r["bytes"].values())
        print(f"week {wk}: {os.path.basename(p)} · t0 = tick {r['t0']} ({r['t0_from']}) · "
              f"parse {r['parse_s']:.2f} s · parse+write {r['total_s']:.2f} s · "
              f"{jsonl_bytes(p):,} JSONL bytes -> {tot:,} Parquet bytes")
        for k in SCHEMAS:
            print(f"   {k:<16} {r['rows'][k]:>9,} rows {r['bytes'].get(k, 0):>12,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
