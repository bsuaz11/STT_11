#!/usr/bin/env python3
"""backtest.py — my strategy vs three baselines on the Parquet tape.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from collections import defaultdict

import polars as pl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import backtest_parquet as bp  
from backtest_parquet import EQUITIES, Book, Signal, Strategy, Tape  # noqa: E402

SPLIT = 650


class RampTrail(Strategy):
    name = "ramp_trail"

    def __init__(self, size: int = 200, trail: float = 0.015, max_hold: int = 120,
                 fee: float = 0.003, cap: int = 400) -> None:
        self.size, self.trail, self.max_hold, self.fee, self.cap = size, trail, max_hold, fee, cap
        self.new: list[dict] = []
        self.plans: list[dict] = []

    def on_event(self, ev: dict, now: int) -> None:
        if ev.get("event") != "CALENDAR_EVENT" or ev.get("pct") is None:
            return
        d = json.loads(ev["data"])
        pct = float(ev["pct"])
        over = float(d.get("overshoot") or 1.0)
        ramp = int(d.get("ramp_ticks") or 15)
        if abs(pct) * over <= 2 * self.fee:                       
            return
        for s in d.get("affected") or []:
            if s in EQUITIES:
                self.new.append({"sym": s, "pct": pct, "over": over, "ramp": ramp, "t0": now})

    def on_tick(self, now, market, pf):
        for p in self.new:                       
            b = market.book(p["sym"])
            if b is None:
                continue
            base = pf.working(p["sym"])
            p.update(base=base, limit=round(b.mid * (1 + p["pct"] * p["over"]), 2),
                     peak=b.mid, phase="enter",
                     target=min(base + self.size, self.cap) if p["pct"] > 0 else 0)
            self.plans.append(p)
        self.new = []
        out, keep = [], []
        for p in self.plans:
            s, pos, b = p["sym"], pf.working(p["sym"]), market.book(p["sym"])
            age = now - p["t0"]
            if p["phase"] == "enter":
                if age < p["ramp"]:
                    if p["pct"] > 0 and pos < p["target"]:
                        out.append(Signal(symbol=s, side="buy", quantity=p["target"] - pos,
                                          price=p["limit"]))
                    elif p["pct"] < 0 and pos > 0:
                        out.append(Signal(symbol=s, side="sell", quantity=pos, price=p["limit"]))
                    keep.append(p)
                    continue
                p["phase"] = "hold" if p["pct"] > 0 else "exit"
            if p["phase"] == "hold":                         
                p["peak"] = max(p["peak"], b.mid)
                if b.mid > p["peak"] * (1 - self.trail) and age < self.max_hold:
                    keep.append(p)
                    continue
                p["phase"] = "exit"
            if pf.inflight[s]:                        
                keep.append(p)
            elif pos > p["base"] and b.best_bid:                   
                out.append(Signal(symbol=s, side="sell", quantity=pos - p["base"],
                                  price=round(b.best_bid * 0.997, 2)))
                keep.append(p)
            elif pos < p["base"] and b.best_ask:
                out.append(Signal(symbol=s, side="buy", quantity=p["base"] - pos,
                                  price=round(b.best_ask * 1.003, 2)))
                keep.append(p)
        self.plans = keep
        return out


def load_tape(W: str, week: int, t_from: int | None = None, t_to: int | None = None) -> Tape:
    """The lab's load_tape, for this warehouse (ts stored as float epoch seconds)."""
    def scan(kind):
        lf = pl.scan_parquet(f"{W}/{kind}/**/*.parquet", hive_partitioning=True)
        lf = lf.filter(pl.col("week") == week)
        if t_from is not None:
            lf = lf.filter(pl.col("t") >= t_from)
        if t_to is not None:
            lf = lf.filter(pl.col("t") <= t_to)
        return lf

    book = (scan("book_snapshot").filter(pl.col("symbol").is_in(EQUITIES))
            .select("seq", "ts", "t", "symbol", "mid", "bid_px", "bid_qty", "ask_px", "ask_qty")
            .sort("seq").collect())
    ev = scan("session_event").sort("seq").collect()
    books_at: dict[int, list] = defaultdict(list)
    ts_at: dict[int, float] = {}
    if t_from is not None:
        warm = (pl.scan_parquet(f"{W}/book_snapshot/**/*.parquet", hive_partitioning=True)
                .filter((pl.col("week") == week) & pl.col("symbol").is_in(EQUITIES)
                        & (pl.col("t") < t_from))
                .sort("seq").group_by("symbol").last()
                .select("symbol", "mid", "bid_px", "bid_qty", "ask_px", "ask_qty")
                .sort("symbol").collect())
        for sym, mid, bpx, bq, apx, aq in warm.iter_rows():
            books_at[t_from].append((sym, Book(t_from - 1, mid, list(zip(bpx, bq)),
                                               list(zip(apx, aq)))))
    for seq, ts, t, sym, mid, bpx, bq, apx, aq in book.iter_rows():
        books_at[t].append((sym, Book(t, mid, list(zip(bpx, bq)), list(zip(apx, aq)))))
        ts_at.setdefault(t, ts)
    events_at: dict[int, list] = defaultdict(list)
    for r in ev.iter_rows(named=True):
        events_at[r["t"]].append(r)
        ts_at.setdefault(r["t"], r["ts"])
    return Tape(list(range(min(books_at), max(books_at) + 1)), books_at, events_at, ts_at)


def weeks_in(W: str) -> list[int]:
    return sorted(int(d.split("=")[1]) for d in os.listdir(f"{W}/book_snapshot")
                  if d.startswith("week="))


def make(name: str, fee: float):
    return {"ramp_trail": lambda: RampTrail(fee=fee),
            "do_nothing": bp.DoNothing, "buy_hold": bp.BuyAndHold,
            "momentum": lambda: bp.TickMomentum(lookback=20)}[name]()


def sharpe(eq: list[float]) -> float:
    d = [b - a for a, b in zip(eq, eq[1:])]
    n = len(d)
    mu = sum(d) / n
    var = sum((x - mu) ** 2 for x in d) / (n - 1)
    return mu / math.sqrt(var) if var > 0 else float("nan")


def row(wk, r: bp.Result, label=None) -> str:
    return (f"| {wk} | {label or r.name} | {r.pnl:+,.0f} | {r.pnl_is:+,.0f} | {r.pnl_oos:+,.0f} | "
            f"{r.max_dd:,.0f} | {sharpe(r.equity):+.4f} | {r.fills} | {r.fees:,.0f} | "
            f"`{r.digest}` |")


HEAD = ("| week | strategy | P&L $ | IS t<{s} $ | OOS t>={s} $ | max DD $ | Sharpe/tick (ddof=1) "
        "| fills | fees $ | fill digest |\n|---:|---|---:|---:|---:|---:|---:|---:|---:|---|")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--warehouse", default="stores/arena.parquet")
    ap.add_argument("--week", type=int, action="append", help="default: every week")
    ap.add_argument("--fee", type=float, default=0.003)
    ap.add_argument("--split", type=int, default=SPLIT)
    ap.add_argument("--speeds", type=float, nargs="*", default=None,
                    help="replay RampTrail at these speeds (inf = no pacing; 50 = 50x real time)")
    ap.add_argument("--sensitivity", action="store_true")
    ap.add_argument("--realclock-demo", action="store_true",
                    help="control: the lab's EventRide with its time.time() bug at 2x and 10x")
    a = ap.parse_args()
    weeks = a.week or weeks_in(a.warehouse)
    kw = dict(fee=a.fee, split=a.split)

    for wk in weeks:
        t = time.perf_counter()
        tape = load_tape(a.warehouse, wk)
        print(f"week {wk}: t {tape.ticks[0]}..{tape.ticks[-1]} ({len(tape.ticks):,} ticks) "
              f"loaded in {(time.perf_counter() - t) * 1e3:.0f} ms · fee {a.fee:.2%} · split t={a.split}")

        if a.realclock_demo:    
            sl = load_tape(a.warehouse, wk, 390, 450)
            print("\n| week | strategy (t 390..450) | speed | P&L $ | fills | fill digest |\n"
                  "|---:|---|---:|---:|---:|---|")
            for rc in (False, True):
                for sp in (2.0, 10.0):
                    r = bp.run(bp.EventRide(fee=a.fee, realclock=rc), sl, speed=sp, **kw)
                    print(f"| {wk} | lab EventRide{' --realclock (time.time() exit)' if rc else ''}"
                          f" | {sp:g}x | {r.pnl:+,.2f} | {r.fills} | `{r.digest}` |")
            continue

        if a.speeds:
            print("\n| week | speed | P&L $ | fills | fees $ | fill digest | wall s |\n"
                  "|---:|---:|---:|---:|---:|---|---:|")
            for sp in a.speeds:
                r = bp.run(make("ramp_trail", a.fee), tape, speed=sp, **kw)
                print(f"| {wk} | {'no pacing' if math.isinf(sp) else f'{sp:g}x'} | {r.pnl:+,.2f} | "
                      f"{r.fills} | {r.fees:,.2f} | `{r.digest}` | {r.wall:.1f} |")
            continue

        if a.sensitivity:
            print("\n" + HEAD.format(s=a.split))
            for lat in (0, 1, 2):
                r = bp.run(make("ramp_trail", a.fee), tape, latency=lat, **kw)
                print(row(wk, r, f"ramp_trail · latency {lat} · sweep"))
            for mode in ("touch",):
                r = bp.run(make("ramp_trail", a.fee), tape, mode=mode, **kw)
                print(row(wk, r, f"ramp_trail · latency 1 · {mode}"))
            r = bp.run(make("ramp_trail", a.fee), tape, **dict(kw, fee=0.0))
            print(row(wk, r, "ramp_trail · latency 1 · sweep · NO FEE (for scale)"))
            continue

        print("\n" + HEAD.format(s=a.split))
        for n in ("ramp_trail", "do_nothing", "buy_hold", "momentum"):
            r = bp.run(make(n, a.fee), tape, **kw)
            print(row(wk, r))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
