#!/usr/bin/env python3
"""run_queries.py — the same questions asked of both stores, timed, answers compared.

    python run_queries.py --team <your_team_id> [--db stores/arena.sqlite] [--warehouse stores/arena.parquet]

"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import time

import duckdb

# ─────────────────────────────────────────────────────────────────────────────
# SQLite
# ─────────────────────────────────────────────────────────────────────────────
SQ_ASOF_BACK = ("(SELECT b.mid FROM book_snapshot b WHERE b.week = {w} AND b.symbol = {s} "
                "AND b.t <= {at} ORDER BY b.t DESC, b.seq DESC LIMIT 1)")
SQLITE = {
    "Q1 event windows": """
        WITH ev AS (
            SELECT e.week, e.t AS ev_t, e.kind, e.pct, j.value AS symbol
            FROM session_event e, json_each(e.data, '$.affected') j
            WHERE e.event = 'CALENDAR_EVENT')
        SELECT week, kind, symbol, ev_t, pct,
               """ + SQ_ASOF_BACK.format(w = "ev.week", s = "ev.symbol", at = "ev.ev_t - 1") + """ AS pre,
               """ + SQ_ASOF_BACK.format(w = "ev.week", s = "ev.symbol", at = "ev.ev_t + 5") + """ AS m5,
               """ + SQ_ASOF_BACK.format(w = "ev.week", s = "ev.symbol", at = "ev.ev_t + 15") + """ AS m15,
               """ + SQ_ASOF_BACK.format(w = "ev.week", s = "ev.symbol", at = "ev.ev_t + 60") + """ AS m60
        FROM ev ORDER BY week, ev_t, symbol""",

    # backward
    "Q2a my fills: prevailing book (backward)": """
        WITH f AS (
            SELECT week, seq, t, symbol, price, quantity,
                   CASE WHEN buyer_id IN (SELECT value FROM json_each(:team)) THEN 1 ELSE -1 END AS sgn,
                   CASE WHEN taker_id IN (SELECT value FROM json_each(:team)) THEN 'taker' ELSE 'maker' END AS role
            FROM trade_execution WHERE taker_id IN (SELECT value FROM json_each(:team)) OR maker_id IN (SELECT value FROM json_each(:team))),
        x AS (
            SELECT f.*, (SELECT b.seq FROM book_snapshot b
                         WHERE b.week = f.week AND b.symbol = f.symbol
                           AND b.t <= f.t AND b.seq < f.seq
                         ORDER BY b.t DESC, b.seq DESC LIMIT 1) AS bseq
            FROM f)
        SELECT x.week, x.seq, x.t, x.symbol, x.role, x.sgn, x.price, x.quantity,
               b.best_bid, b.best_ask
        FROM x LEFT JOIN book_snapshot b ON b.week = x.week AND b.seq = x.bseq
        ORDER BY x.week, x.seq""",

    
    "Q2b my fills: markout +5/+60 (forward)": """
        WITH f AS (
            SELECT week, seq, t, symbol, price, quantity,
                   CASE WHEN buyer_id IN (SELECT value FROM json_each(:team)) THEN 1 ELSE -1 END AS sgn
            FROM trade_execution WHERE taker_id IN (SELECT value FROM json_each(:team)) OR maker_id IN (SELECT value FROM json_each(:team))),
        g AS (
            SELECT f.*,
              (SELECT min(b.t) FROM book_snapshot b WHERE b.week = f.week
                 AND b.symbol = f.symbol AND b.t >= f.t + 5)  AS t5,
              (SELECT min(b.t) FROM book_snapshot b WHERE b.week = f.week
                 AND b.symbol = f.symbol AND b.t >= f.t + 60) AS t60
            FROM f)
        SELECT g.week, g.seq, g.t, g.symbol, g.sgn, g.price, g.quantity,
          (SELECT b.mid FROM book_snapshot b INDEXED BY book_wst WHERE b.week = g.week
             AND b.symbol = g.symbol AND b.t = g.t5  ORDER BY b.seq DESC LIMIT 1) AS mid5,
          (SELECT b.mid FROM book_snapshot b INDEXED BY book_wst WHERE b.week = g.week
             AND b.symbol = g.symbol AND b.t = g.t60 ORDER BY b.seq DESC LIMIT 1) AS mid60
        FROM g ORDER BY g.week, g.seq""",

    # forward
    "Q2c all fills: markout +60 (forward)": """
        WITH g AS (
            SELECT f.week, f.seq, f.symbol, f.price,
                   CASE WHEN f.aggressor = 'buy' THEN 1 ELSE -1 END AS sgn,
                   (SELECT min(b.t) FROM book_snapshot b WHERE b.week = f.week
                      AND b.symbol = f.symbol AND b.t >= f.t + 60) AS t60
            FROM trade_execution f)
        SELECT count(*), sum(1e4 * sgn * (m60 - price) / price) FROM (
            SELECT g.*, (SELECT b.mid FROM book_snapshot b INDEXED BY book_wst
                         WHERE b.week = g.week AND b.symbol = g.symbol AND b.t = g.t60
                         ORDER BY b.seq DESC LIMIT 1) AS m60 FROM g)
        WHERE m60 IS NOT NULL""",

    "Q3 full scan: spread & touch depth by symbol": """
        SELECT symbol, count(*) AS snapshots,
               avg(CASE WHEN spread IS NOT NULL THEN 1e4 * spread / mid END) AS spread_bp,
               avg(best_bid_qty + best_ask_qty) AS touch_qty
        FROM book_snapshot GROUP BY symbol ORDER BY symbol""",

    "Q4a point: AAPL book at end of t400, week 2": """
        WITH s AS (SELECT seq FROM book_snapshot
                   WHERE week = 2 AND symbol = 'AAPL' AND t = 400
                   ORDER BY seq DESC LIMIT 1)
        SELECT l.side, l.lvl, l.px, l.qty FROM s
        JOIN book_level l ON l.week = 2 AND l.seq = s.seq
        ORDER BY l.side, l.lvl""",

    "Q4b point: my position at end of t1400 (--point-week)": """
        SELECT symbol, sum(CASE WHEN buyer_id IN (SELECT value FROM json_each(:team)) THEN quantity ELSE -quantity END) AS net
        FROM trade_execution
        WHERE (taker_id IN (SELECT value FROM json_each(:team)) OR maker_id IN (SELECT value FROM json_each(:team))) AND week = :pw AND t <= 1400
        GROUP BY symbol ORDER BY symbol""",
}

# ─────────────────────────────────────────────────────────────────────────────
# DuckDB over Parquet
# ─────────────────────────────────────────────────────────────────────────────
DUCK_VIEWS = """
CREATE OR REPLACE VIEW book   AS SELECT * FROM read_parquet('{W}/book_snapshot/**/*.parquet',   hive_partitioning = true);
CREATE OR REPLACE VIEW trades AS SELECT * FROM read_parquet('{W}/trade_execution/**/*.parquet', hive_partitioning = true);
CREATE OR REPLACE VIEW events AS SELECT * FROM read_parquet('{W}/session_event/**/*.parquet',   hive_partitioning = true);
-- the end-of-tick book: the last snapshot of each (week, symbol, t) by seq
CREATE OR REPLACE VIEW book_eot AS
    SELECT * FROM book QUALIFY row_number() OVER (PARTITION BY week, symbol, t ORDER BY seq DESC) = 1;
"""
DUCK = {
    "Q1 event windows": """
        WITH ev AS (
            SELECT week, t AS ev_t, kind, pct, unnest(string_split(affected, ',')) AS symbol
            FROM events WHERE event = 'CALENDAR_EVENT'),
        g AS (SELECT ev.*, k, ev_t + k AS at FROM ev, (VALUES (-1), (5), (15), (60)) v(k)),
        px AS (SELECT g.*, b.mid FROM g ASOF JOIN book_eot b
               ON g.week = b.week AND g.symbol = b.symbol AND g.at >= b.t)
        SELECT week, kind, symbol, ev_t, any_value(pct) AS pct,
               max(mid) FILTER (k = -1) AS pre, max(mid) FILTER (k = 5) AS m5,
               max(mid) FILTER (k = 15) AS m15, max(mid) FILTER (k = 60) AS m60
        FROM px GROUP BY ALL ORDER BY week, ev_t, symbol""",

    "Q2a my fills: prevailing book (backward)": """
        WITH f AS (
            SELECT week, seq, t, symbol, price, quantity,
                   CASE WHEN list_contains($team, buyer_id) THEN 1 ELSE -1 END AS sgn,
                   CASE WHEN list_contains($team, taker_id) THEN 'taker' ELSE 'maker' END AS role
            FROM trades WHERE list_contains($team, taker_id) OR list_contains($team, maker_id))
        SELECT f.week, f.seq, f.t, f.symbol, f.role, f.sgn, f.price, f.quantity,
               b.best_bid, b.best_ask
        FROM f ASOF LEFT JOIN book b
          ON f.week = b.week AND f.symbol = b.symbol AND f.seq > b.seq
        ORDER BY f.week, f.seq""",

    "Q2b my fills: markout +5/+60 (forward)": """
        WITH f AS (
            SELECT week, seq, t, symbol, price, quantity,
                   CASE WHEN list_contains($team, buyer_id) THEN 1 ELSE -1 END AS sgn
            FROM trades WHERE list_contains($team, taker_id) OR list_contains($team, maker_id)),
        e AS (SELECT week, symbol, t, mid FROM book_eot
              WHERE symbol IN (SELECT DISTINCT symbol FROM f)),
        a AS (SELECT f.*, e.mid AS mid5 FROM f ASOF LEFT JOIN e
              ON f.week = e.week AND f.symbol = e.symbol AND e.t >= f.t + 5)
        SELECT a.week, a.seq, a.t, a.symbol, a.sgn, a.price, a.quantity, a.mid5, e.mid AS mid60
        FROM a ASOF LEFT JOIN e ON a.week = e.week AND a.symbol = e.symbol AND e.t >= a.t + 60
        ORDER BY a.week, a.seq""",

    "Q2c all fills: markout +60 (forward)": """
        SELECT count(*), sum(1e4 * f.sgn * (e.mid - f.price) / f.price) FROM (
            SELECT week, t, symbol, price, CASE WHEN aggressor = 'buy' THEN 1 ELSE -1 END AS sgn
            FROM trades) f
        ASOF JOIN book_eot e ON f.week = e.week AND f.symbol = e.symbol AND e.t >= f.t + 60""",

    "Q3 full scan: spread & touch depth by symbol": """
        SELECT symbol, count(*) AS snapshots,
               avg(1e4 * spread / mid) FILTER (spread IS NOT NULL) AS spread_bp,
               avg(best_bid_qty + best_ask_qty) AS touch_qty
        FROM book GROUP BY symbol ORDER BY symbol""",

    "Q4a point: AAPL book at end of t400, week 2": """
        WITH s AS (SELECT bid_px, bid_qty, ask_px, ask_qty FROM book
                   WHERE week = 2 AND symbol = 'AAPL' AND t = 400
                   ORDER BY seq DESC LIMIT 1)
        SELECT 'A' AS side, i - 1 AS lvl, ask_px[i] AS px, ask_qty[i] AS qty
        FROM s, range(1, len(ask_px) + 1) r(i)
        UNION ALL
        SELECT 'B', i - 1, bid_px[i], bid_qty[i] FROM s, range(1, len(bid_px) + 1) r(i)
        ORDER BY side, lvl""",

    "Q4b point: my position at end of t1400 (--point-week)": """
        SELECT symbol, sum(CASE WHEN list_contains($team, buyer_id) THEN quantity ELSE -quantity END) AS net
        FROM trades
        WHERE (list_contains($team, taker_id) OR list_contains($team, maker_id)) AND week = $pw AND t <= 1400
        GROUP BY symbol ORDER BY symbol""",
}


def best_of(fn, n):
    times, out = [], None
    for _ in range(n):
        t = time.perf_counter()
        out = fn()
        times.append(time.perf_counter() - t)
    return times[0], min(times), out


def norm(rows):
    def v(x):
        if isinstance(x, float):
            return None if math.isnan(x) else round(x, 6)
        return x
    return [tuple(v(x) for x in r) for r in rows]


def ms(s):
    return f"{s * 1e3:,.2f} ms" if s < 0.1 else f"{s * 1e3:,.0f} ms"


def summarise(name, rows):
    """Human-readable answer for the report."""
    if name.startswith("Q1"):
        out = []
        for wk, kind, sym, ev_t, pct, pre, m5, m15, m60 in rows:
            r = lambda m: f"{100 * (m / pre - 1):+.2f}%" if (m and pre) else "—"  # noqa: E731
            out.append(f"w{wk} t{ev_t} {kind:<10} {sym:<7} announced {100 * pct:+.2f}% · "
                       f"pre {pre:.2f} · +5 {r(m5)} · +15 {r(m15)} · +60 {r(m60)}")
        return out
    if name.startswith("Q2a"):
        d = [r for r in rows if r[8] is not None and r[9] is not None]
        if not d:
            return [f"{len(rows)} fills"]
        cost = [1e4 * r[5] * (r[6] - (r[8] + r[9]) / 2) / ((r[8] + r[9]) / 2) for r in d]
        tk = [c for c, r in zip(cost, d) if r[4] == "taker"]
        mk = [c for c, r in zip(cost, d) if r[4] == "maker"]
        s = (f"{len(rows)} fills, {len(d)} against a two-sided book ({len(tk)} taker, "
             f"{len(mk)} maker) · mean paid vs touch mid "
             f"{sum(cost) / len(cost):+.2f} bp")
        if tk:
            s += f" · taker {sum(tk) / len(tk):+.2f} bp"
        if mk:
            s += f" · maker {sum(mk) / len(mk):+.2f} bp"
        return [s]
    if name.startswith("Q2b"):
        out = []
        for k, col in ((5, 7), (60, 8)):
            d = [r for r in rows if r[col] is not None]
            if d:
                bp = sum(1e4 * r[4] * (r[col] - r[5]) / r[5] for r in d) / len(d)
                usd = sum(r[4] * (r[col] - r[5]) * r[6] for r in d)
                out.append(f"+{k} ticks: {len(d)} fills · mean {bp:+.2f} bp · sum ${usd:+,.2f}")
        return out or [f"{len(rows)} fills"]
    if name.startswith("Q2c"):
        n, tot = rows[0]
        return [f"{n:,} fills with a book 60 ticks later · mean taker markout {tot / n:+.3f} bp"]
    if name.startswith("Q3"):
        return [f"{s:<8} {n:>6,} snapshots · spread {sp:6.1f} bp · touch qty {q:7.1f}"
                if sp is not None else f"{s:<8} {n:>6,} snapshots · one-sided only"
                for s, n, sp, q in rows]
    if name.startswith("Q4a"):
        return [" ".join(f"{sd}{lv}:{px:.2f}x{q:g}" for sd, lv, px, q in rows)]
    return [", ".join(f"{s} {n:+d}" for s, n in rows) or "no fills by then"]


def main() -> int:
    ap = argparse.ArgumentParser(description = __doc__.split("\n")[0])
    ap.add_argument("--team", required=True,
                    help = "your team_id(s) as on the tape, comma-separated (all seats count as 'me')")
    ap.add_argument("--point-week", type = int, default = None,
                    help = "week for the Q4b position lookup (default: the last week loaded)")
    ap.add_argument("--db", default = "stores/arena.sqlite")
    ap.add_argument("--warehouse", default = "stores/arena.parquet")
    ap.add_argument("--repeat", type = int, default = 7)
    ap.add_argument("--plans", action = "store_true", help = "print SQLite query plans")
    a = ap.parse_args()

    sq = sqlite3.connect(a.db)
    dk = duckdb.connect()
    dk.execute(DUCK_VIEWS.replace("{W}", a.warehouse))
    weeks = [w for (w,) in sq.execute("SELECT week FROM load_log ORDER BY week")]
    team = [x.strip() for x in a.team.split(",") if x.strip()]
    pw = a.point_week if a.point_week is not None else weeks[-1]
    sp = {"team": json.dumps(team), "pw": pw}
    print(f"sqlite {sqlite3.sqlite_version} · duckdb {duckdb.__version__} · team {a.team} · "
          f"weeks {weeks} · Q4b week {pw} · best of {a.repeat}")
    rows = []
    for name in SQLITE:
        f_sq, b_sq, o_sq = best_of(lambda: sq.execute(SQLITE[name], sp).fetchall(), a.repeat)
        dp = {k: v for k, v in (("team", team), ("pw", pw)) if f"${k}" in DUCK[name]} or None
        f_dk, b_dk, o_dk = best_of(lambda: dk.execute(DUCK[name], dp).fetchall(), a.repeat)
        same = norm(o_sq) == norm(o_dk)
        rows.append((name, len(o_sq), f_sq, b_sq, f_dk, b_dk, same))
        print(f"\n{name}  ({len(o_sq):,} rows; same answer in both stores: {same})")
        for line in summarise(name, o_sq):
            print("   " + line)
        if not same:
            print("   DuckDB: " + " | ".join(summarise(name, o_dk)))
        if a.plans:
            for r in sq.execute("EXPLAIN QUERY PLAN " + SQLITE[name], sp):
                print("   plan:", r[-1])

    print("\n| query | rows | SQLite best | SQLite 1st | DuckDB/Parquet best | DuckDB 1st | "
          "faster | same |")
    print("|---|---:|---:|---:|---:|---:|---|---|")
    for name, n, f_sq, b_sq, f_dk, b_dk, same in rows:
        win = (f"SQLite {b_dk / b_sq:.1f}x" if b_sq < b_dk else f"DuckDB {b_sq / b_dk:.1f}x")
        print(f"| {name} | {n:,} | {ms(b_sq)} | {ms(f_sq)} | {ms(b_dk)} | {ms(f_dk)} | {win} | "
              f"{'yes' if same else 'NO'} |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
