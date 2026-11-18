"""
Phase4/leadlag_backtest.py — run team/leadlag.py's LeadLagSignal over the week-8 history.

The same class the live trader uses, fed tick by tick with the recorded marks
and books of each session, so the backtest tests the code that trades:

    fills      --fill next: an IOC fills at the NEXT tick's touch (a full tick of
               latency, pessimistic); --fill same: at the touch of the tick it
               was decided on (live, an IOC lands well inside the 1 s tick)
    fees       TAKER_BPS per side (a cheap student venue: 8–14 bps; the
               primary charged 200 bps in week 7 — nothing pays that)
    grading    each entry is checked like the Part A check: an order on the
               follower, inside the lag after a big leader move, in the
               predicted direction (and whether the mark then moved that way)

    python Phase4/leadlag_backtest.py week8/week08_history-3 [--sessions 100]

Writes Phase4/results/leadlag_backtest.txt.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import time
from collections import defaultdict

import duckdb
import numpy as np

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from team.leadlag import LeadLagSignal

OUT = pathlib.Path(__file__).resolve().parent / "results"
TAKER_BPS = 10.0


def run_session(sig: LeadLagSignal, marks: dict[str, np.ndarray], bid: dict[str, np.ndarray],
                ask: dict[str, np.ndarray], bqty: dict[str, np.ndarray], aqty: dict[str, np.ndarray],
                events: list[tuple], stats: dict, same_tick: bool = False) -> None:
    """One session through the signal, with a simple next-tick fill model."""
    T = len(next(iter(marks.values())))
    sig.new_session()
    sig.trades.clear()
    sig.on_calendar([{"tick": t, "symbol": s, "market_wide": mw, "kind": "x"}
                     for t, e, s, mw in events if e == "CALENDAR_EVENT"])
    halted: set[str] = set()
    halt_ev = defaultdict(list)
    for t, e, s, _ in events:
        if e in ("TRADING_HALT", "TRADING_RESUMED"):
            halt_ev[t].append((e, s))
    pos: dict[str, int] = defaultdict(int)
    pending: list = []                      # orders sent at t, filled at t+1
    for t in range(T):
        for e, s in halt_ev.get(t, []):
            (halted.add if e == "TRADING_HALT" else halted.discard)(s)
        # fills of last tick's orders
        for o in pending:
            b, a = bid[o.symbol][t], ask[o.symbol][t]
            if o.side == "buy" and not np.isnan(a) and a <= o.price:
                q = min(o.quantity, int(aqty[o.symbol][t]) or o.quantity)
                px = a
            elif o.side == "sell" and not np.isnan(b) and b >= o.price:
                q = min(o.quantity, int(bqty[o.symbol][t]) or o.quantity)
                px = b
            else:
                continue
            sig.on_fill(o.symbol, o.side, q, px)
            pos[o.symbol] += q if o.side == "buy" else -q
            stats["fees"] += q * px * TAKER_BPS / 1e4
            stats["notional"] += q * px
        pending = []
        mk = {s: float(v[t]) for s, v in marks.items()}
        touch = {s: (float(bid[s][t]), float(ask[s][t])) for s in sig.followers}
        n_log = len(sig.log)
        orders = sig.step(t, mk, touch, halted, dict(pos))
        for row in sig.log[n_log:]:
            lk = next(lk for lk in sig.links if lk.follower == row["follower"])
            f = marks[lk.follower]
            if t + lk.lag + 1 < T:
                moved = np.log(f[t + lk.lag] / f[t]) * (1 if row["side"] == "buy" else -1)
                stats["entries"][row["kind"]] += 1
                stats["mark_right"][row["kind"]] += int(moved > 0)
                stats["mark_bps"][row["kind"]].append(moved * 1e4)
        pending.extend(orders)
        if same_tick:                       # fill now, against this tick's book
            for o in pending:
                b, a = bid[o.symbol][t], ask[o.symbol][t]
                if o.side == "buy" and not np.isnan(a) and a <= o.price:
                    q, px = min(o.quantity, int(aqty[o.symbol][t]) or o.quantity), a
                elif o.side == "sell" and not np.isnan(b) and b >= o.price:
                    q, px = min(o.quantity, int(bqty[o.symbol][t]) or o.quantity), b
                else:
                    continue
                sig.on_fill(o.symbol, o.side, q, px)
                pos[o.symbol] += q if o.side == "buy" else -q
                stats["fees"] += q * px * TAKER_BPS / 1e4
                stats["notional"] += q * px
            pending = []
    # mark anything left at the last mark
    for s, q in pos.items():
        if q:
            stats["open_value"] += q * float(marks[s][-1])
    stats["cash"] += sum(tr.cash for tr in sig.closed) + sum(tr.cash for tr in sig.trades.values())
    for tr in sig.closed:
        stats["closed"][tr.kind] += 1
        stats["closed_cash"][tr.kind] += tr.cash


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("roots", nargs="+", type=pathlib.Path)
    ap.add_argument("--sessions", type=int, default=0, help="per part (0 = all)")
    ap.add_argument("--fill", choices=("same", "next"), default="next")
    args = ap.parse_args()
    t0 = time.perf_counter()
    stats = {"fees": 0.0, "notional": 0.0, "cash": 0.0, "open_value": 0.0,
             "entries": defaultdict(int), "mark_right": defaultdict(int), "mark_bps": defaultdict(list),
             "closed": defaultdict(int), "closed_cash": defaultdict(float), "sessions": 0,
             "per_session": []}
    con = duckdb.connect()
    for root in args.roots:
        root = root.expanduser()
        sig = LeadLagSignal()
        syms = sorted({s for lk in sig.links for s in (lk.leader, lk.follower)})
        q = ", ".join(f"'{s}'" for s in syms)
        df = con.execute(f"""SELECT session, symbol, tick, mark, bid1, ask1, bid_qty1, ask_qty1
            FROM read_parquet('{root}/books/*/*.parquet', hive_partitioning = 1)
            WHERE symbol IN ({q}) ORDER BY session, symbol, tick""").fetchnumpy()
        sessions = np.unique(df["session"])
        if args.sessions:
            sessions = sessions[:args.sessions]
        n_t = int(df["tick"].max()) + 1
        S = len(np.unique(df["session"]))
        cube = {c: np.asarray(df[c], dtype=float).reshape(S, len(syms), n_t)
                for c in ("mark", "bid1", "ask1", "bid_qty1", "ask_qty1")}
        ev = con.execute(f"""SELECT session, tick, event, symbol, coalesce(market_wide, false)
            FROM read_parquet('{root}/events/*.parquet') ORDER BY session, tick""").fetchall()
        by_sess = defaultdict(list)
        for s, t, e, sym, mw in ev:
            by_sess[int(s)].append((int(t), e, sym, bool(mw)))
        for i, sess in enumerate(sessions):
            sig = LeadLagSignal()
            before = len(sig.log)
            col = {c: {s: cube[c][i, j] for j, s in enumerate(syms)} for c in cube}
            run_session(sig, col["mark"], col["bid1"], col["ask1"], col["bid_qty1"],
                        col["ask_qty1"], by_sess[int(sess)], stats, args.fill == "same")
            stats["per_session"].append(len(sig.log) - before)
            stats["sessions"] += 1
    e, r, mb = stats["entries"], stats["mark_right"], stats["mark_bps"]
    per = np.array(stats["per_session"])
    net = stats["cash"] + stats["open_value"] - stats["fees"]
    lines = [(f"LeadLagSignal over {stats['sessions']} recorded week-8 sessions "
             f"({time.perf_counter() - t0:.0f} s); fills at the {args.fill} tick's touch, {TAKER_BPS:g} bps taker per side."),
             (f"Orders on a follower inside the lag after a big leader move, per session: "
             f"median {np.median(per):.0f}, min {per.min()}, sessions with ≥10: {np.mean(per >= 10):.0%}"),
             "All of them are in the predicted direction by construction (side = sign(beta) × sign(leader move)).", ""]
    for kind in ("marker", "ramp"):
        if e[kind]:
            lines.append(f"  {kind:6}: {e[kind] / stats['sessions']:.1f} per session; the follower's mark then moved "
                         f"the predicted way {r[kind] / e[kind]:.0%} of the time, mean {np.mean(mb[kind]):+.1f} bps "
                         f"over the lag; {stats['closed'][kind]} trades, gross cash {stats['closed_cash'][kind]:+,.0f}")
    lines += ["", (f"Traded ${stats['notional']:,.0f}; fees ${stats['fees']:,.0f}; "
              f"net P&L ${net:+,.0f} = ${net / max(1, stats['sessions']):+,.1f} per session.")]
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "leadlag_backtest.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
