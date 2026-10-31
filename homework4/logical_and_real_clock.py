#!/usr/bin/env python3
"""clock_page.py — the numbers for the "logical vs real clock" page, from your own tape.

    python clock_page.py --warehouse stores/arena.parquet --week 4 --team <your_team_id> \
        [--bot path/to/your/bot] [--strategy mybot_strategy:MyStrategy]
"""
from __future__ import annotations

import argparse
import ast
import importlib
import os
import statistics
import sys

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

TIME_CALLS = {("time", "time"), ("time", "monotonic"), ("time", "perf_counter"),
              ("time", "time_ns"), ("time", "monotonic_ns"), ("time", "sleep"),
              ("datetime", "now"), ("datetime", "utcnow"), ("datetime", "today"),
              ("asyncio", "sleep"), ("loop", "time")}


def scan_bot(root: str) -> list[tuple]:
    hits = []
    files = [root] if root.endswith(".py") else [
        os.path.join(d, f) for d, _, fs in os.walk(root) for f in fs if f.endswith(".py")]
    for path in sorted(files):
        try:
            tree = ast.parse(open(path, encoding="utf-8").read(), path)
        except SyntaxError as e:
            hits.append((path, e.lineno, "?", f"(could not parse: {e.msg})"))
            continue
        parents = {}
        for node in ast.walk(tree):
            for ch in ast.iter_child_nodes(node):
                parents[ch] = node
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            v = node.func.value
            base = v.id if isinstance(v, ast.Name) else v.attr if isinstance(v, ast.Attribute) else None
            if (base, node.func.attr) in TIME_CALLS or (node.func.attr in ("time", "monotonic")
                                                      and base == "time"):
                fn, p = "<module>", node
                while p in parents:
                    p = parents[p]
                    if isinstance(p, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        fn = p.name
                        break
                hits.append((path, node.lineno, fn, f"{base}.{node.func.attr}()"))
    return hits


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--warehouse", default = "stores/arena.parquet")
    ap.add_argument("--week", type = int, required = True)
    ap.add_argument("--team", required = True)
    ap.add_argument("--bot", default = None, help = "folder or .py file of your bot")
    ap.add_argument("--strategy", default = None, help = "module:Class to replay (Strategy API)")
    a = ap.parse_args()
    W, wk, team = a.warehouse, a.week, a.team
    c = duckdb.connect()
    for v, d in (("book", "book_snapshot"), ("trades", "trade_execution"), ("acks", "order_ack"),
                 ("events", "session_event"), ("board", "leaderboard"),
                 ("rejects", "order_reject")):
        if os.path.isdir(f"{W}/{d}/week={wk}"):
            c.execute(f"CREATE VIEW {v} AS SELECT * FROM read_parquet('{W}/{d}/**/*.parquet', "
                      f"hive_partitioning = true) WHERE week = {wk}")
        else:                
            c.execute(f"CREATE VIEW {v} AS SELECT * FROM (SELECT NULL::VARCHAR AS team_id, "
                      f"NULL::BIGINT AS seq, NULL::INTEGER AS t, NULL::DOUBLE AS ts, "
                      f"NULL::VARCHAR AS symbol, NULL::VARCHAR AS side, NULL::VARCHAR AS code, "
                      f"NULL::DOUBLE AS net_worth, NULL::DOUBLE AS fees_paid, "
                      f"NULL::INTEGER AS trade_count) WHERE false")
            print(f"(no {d} on this tape)")

    # 1. wall-clock length of a tick: first ts of each tick, then deltas
    tt = c.execute("""
        SELECT t, min(ts) FROM (SELECT t, ts FROM book UNION ALL SELECT t, ts FROM trades)
        GROUP BY t ORDER BY t""").fetchall()
    ts = [r[1] for r in tt]
    t_first, t_last = c.execute("SELECT min(t), max(t) FROM book").fetchone()
    # seconds per tick between consecutive ticks that have a line; a gap over k quiet
    # ticks counts as k ticks of (gap / k) seconds each
    d = sorted(x for (ta, sa), (tb, sb) in zip(tt, tt[1:]) for x in [(sb - sa) / (tb - ta)] * (tb - ta))
    q = lambda p: d[min(len(d) - 1, int(p * len(d)))]  # noqa: E731
    print(f"week {wk} · team {team}")
    print(f"\n1. wall-clock length of one tick (t {t_first}..{t_last}, {len(d):,} ticks, quiet ticks share their gap)")
    print(f"   block {ts[-1] - ts[0]:,.1f} s for {t_last - t_first:,} ticks = "
          f"{(ts[-1] - ts[0]) / (t_last - t_first):.3f} s/tick on average")
    print(f"   per tick: median {statistics.median(d):.3f} s · p10 {q(.10):.3f} · "
          f"p90 {q(.90):.3f} · p99 {q(.99):.3f} · max {d[-1]:.3f} s")
    for lo, hi in ((0, 0.9), (0.9, 1.1), (1.1, 1.5), (1.5, 1e9)):
        n = sum(lo <= x < hi for x in d)
        print(f"     {lo:.1f}–{'∞' if hi > 1e8 else f'{hi:.1f}'} s: {n:>5,} ticks ({100 * n / len(d):.1f} %)")

    # 2. live fills
    f = c.execute("""
        SELECT t, seq, symbol, price, quantity, buyer_id = $team AS buy, taker_id = $team AS taker,
               CASE WHEN taker_id = $team THEN fee ELSE -maker_rebate END AS cost
        FROM trades WHERE taker_id = $team OR maker_id = $team ORDER BY seq""",
                  {"team": team}).fetchall()
    print(f"\n2. LIVE fills on the tape: {len(f)} "
          f"({sum(1 for r in f if r[6])} as taker, {sum(1 for r in f if not r[6])} as maker)")
    if f:
        for side, flag in (("buy", True), ("sell", False)):
            s = [r for r in f if r[5] == flag]
            if s:
                qty = sum(r[4] for r in s)
                print(f"   {side:<4} {len(s):>4} fills · {qty:>6,} shares · avg price "
                      f"${sum(r[3] * r[4] for r in s) / qty:,.2f}")
        last = dict(c.execute("SELECT symbol, arg_max(mid, seq) FROM book GROUP BY symbol").fetchall())
        cash, pos = 0.0, {}
        for t, seq, sym, px, qty, buy, taker, cost in f:
            sg = 1 if buy else -1
            cash -= sg * px * qty + (cost or 0.0)
            pos[sym] = pos.get(sym, 0) + sg * qty
        mtm = sum(n * last[s] for s, n in pos.items())
        fees = sum((r[7] or 0.0) for r in f)
        print(f"   net fees ${fees:,.2f} (fees − rebates) · open position marked at the last mid "
              f"${mtm:,.2f} · trading P&L ${cash + mtm:+,.2f}")
        print("   by symbol: " + ", ".join(f"{s} {n:+d}" for s, n in sorted(pos.items())))
    b = c.execute("""SELECT arg_min(net_worth, seq), arg_max(net_worth, seq), arg_max(fees_paid, seq),
                            arg_max(trade_count, seq) FROM board WHERE team_id = $team""",
                  {"team": team}).fetchone()
    if b and b[0] is not None:
        print(f"   leaderboard: net worth {b[0]:,.2f} -> {b[1]:,.2f} = {b[1] - b[0]:+,.2f} "
              f"(includes the granted shares' drift) · fees {b[2]:,.2f} · trades {b[3]}")
    rj = c.execute("SELECT code, count(*) FROM rejects WHERE team_id = $team GROUP BY 1 ORDER BY 2 DESC",
                   {"team": team}).fetchall()
    if rj:
        print("   rejects: " + ", ".join(f"{k} {n}" for k, n in rj))

    # 3. reaction to each event, in ticks
    print("\n3. reaction to each CALENDAR_EVENT (logical clock; seconds = wall clock, for scale)")
    evs = c.execute("""SELECT t, ts, kind, pct, affected FROM events
                       WHERE event = 'CALENDAR_EVENT' ORDER BY seq""").fetchall()
    for ev_t, ev_ts, kind, pct, aff in evs:
        syms = (aff or "").split(",")
        ack = c.execute("""SELECT t, ts, symbol, side FROM acks WHERE team_id = $team AND t >= $t
                           AND list_contains($s, symbol) ORDER BY seq LIMIT 1""",
                        {"team": team, "t": ev_t, "s": syms}).fetchone()
        fil = c.execute("""SELECT t, ts FROM trades WHERE (taker_id = $team OR maker_id = $team)
                           AND t >= $t AND list_contains($s, symbol) ORDER BY seq LIMIT 1""",
                        {"team": team, "t": ev_t, "s": syms}).fetchone()
        line = f"   t{ev_t} {kind} {100 * pct:+.2f}% ({len(syms)} names): "
        line += (f"first order ack t{ack[0]} ({ack[2]} {ack[3]}) = +{ack[0] - ev_t} ticks / "
                 f"{ack[1] - ev_ts:+.2f} s" if ack else "no order on an affected name")
        if fil:
            line += (f" · first fill +{fil[0] - ev_t} ticks / {fil[1] - ev_ts:+.2f} s"
                     " (any fill, so it can be an order resting from before)")
        print(line)

    # 4. replay
    if a.strategy:
        import backtest_strategy as B
        import backtest_parquet as bp
        mod, cls = a.strategy.split(":")
        strat = getattr(importlib.import_module(mod), cls)()
        r = bp.run(strat, B.load_tape(W, wk), split=B.SPLIT)
        print(f"\n4. REPLAY of {a.strategy} on the same tape: {r.fills} fills · fees "
              f"${r.fees:,.2f} · P&L ${r.pnl:+,.2f} · digest {r.digest}")
    else:
        print("\n4. REPLAY: pass --strategy module:Class (your on_tick wrapped in the Strategy API)")

    # 5. time calls
    if a.bot:
        hits = scan_bot(a.bot)
        print(f"\n5. time calls in {a.bot}: {len(hits)}")
        for path, line, fn, call in sorted(hits):
            print(f"   {os.path.relpath(path, a.bot if os.path.isdir(a.bot) else os.path.dirname(a.bot))}"
                  f":{line}  in {fn}()  {call}")
    else:
        print("\n5. time calls: pass --bot path/to/your/bot")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
