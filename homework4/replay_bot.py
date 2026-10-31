#!/usr/bin/env python3
"""replay_bot.py — replay the live STT_11 trader bot (team/trader.py) on a recorded tape.

    python replay_bot.py --bot path/to/trader.py --week 4 --seat stt_11_trader_1 --clock tape
    python replay_bot.py --bot path/to/trader.py --week 4 --seat stt_11_trader_1 --clock tick
    python replay_bot.py --bot path/to/trader.py --week 4 --seat stt_11_trader_1 --clock wall --speed 20
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import sys
import time as _real_time
import types
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import polars as pl  # noqa: E402

FEE = 0.003
GRANTED = 20

class _Clock:
    """Stands in for the `time` module inside the bot."""

    def __init__(self, mode: str):
        self.mode, self.now = mode, 0.0

    def monotonic(self):
        return _real_time.monotonic() if self.mode == "wall" else self.now

    def time(self):
        return _real_time.time() if self.mode == "wall" else self.now

    def __getattr__(self, name):          
        return getattr(_real_time, name)


def load_bot(path: str, seat: str, capital: float, clock: _Clock):
    from pydantic import BaseModel

    class Signal(BaseModel):
        type: str = "signal"
        symbol: str
        side: str
        quantity: int
        price: float
        confidence: float = 1.0

    class _Any(BaseModel):
        model_config = {"extra": "allow"}

    class Trader:
        def __init__(self):
            pass

    class _AdaptedTraderBot:
        def __init__(self, owner):
            self._owner = owner

    mods = {
        "trader": types.ModuleType("trader"),
        "trader.config": types.ModuleType("trader.config"),
        "shared": types.ModuleType("shared"),
        "shared.messages": types.ModuleType("shared.messages"),
        "arena": types.ModuleType("arena"),
        "arena.trader": types.ModuleType("arena.trader"),
        "team": types.ModuleType("team"),
        "team.config": types.ModuleType("team.config"),
    }
    mods["trader.config"].TEAM_ID = seat
    mods["trader.config"].STARTING_CASH = capital
    mods["trader"].config = mods["trader.config"]
    mods["shared.messages"].Signal = Signal
    mods["shared.messages"].OrderAck = _Any
    mods["shared.messages"].PlaceOrder = _Any
    mods["arena"].Trader = Trader
    mods["arena.trader"]._AdaptedTraderBot = _AdaptedTraderBot
    mods["team.config"].CAPITAL = {seat: capital}
    sys.modules.update(mods)
    spec = importlib.util.spec_from_file_location("stt11_bot", path)
    bot = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bot)
    bot.time = clock                       
    import logging
    logging.getLogger("stt11_bot").setLevel(logging.ERROR)
    return bot


class Market:
    def __init__(self):
        self.books: dict[str, tuple] = {}
        self.hist: dict[str, list] = defaultdict(list)

    def symbols(self):
        return sorted(self.books)

    def mid_price(self, s):
        b = self.books.get(s)
        return b[0] if b else None

    def best_bid(self, s):
        b = self.books.get(s)
        return b[1][0][0] if b and b[1] else None

    def best_ask(self, s):
        b = self.books.get(s)
        return b[2][0][0] if b and b[2] else None

    def prices(self, s):
        return self.hist[s]


class Portfolio:
    def __init__(self, cash, limit, shorts):
        self.cash, self.limit, self.shorts = cash, limit, shorts
        self.positions: dict[str, int] = {}
        self.realized_pnl = 0.0
        self.total_fees_paid = 0.0

    def can_buy(self, s, q, px):
        return q > 0 and self.cash >= q * px * (1 + FEE) and self.positions.get(s, 0) + q <= self.limit

    def can_sell(self, s, q):
        floor = -self.limit if self.shorts else 0
        return q > 0 and self.positions.get(s, 0) - q >= floor

def load(W, week):
    def scan(kind):
        return (pl.scan_parquet(f"{W}/{kind}/**/*.parquet", hive_partitioning=True)
                .filter(pl.col("week") == week))
    book = (scan("book_snapshot").select("seq", "t", "ts", "symbol", "mid", "ref_price", "bid_px",
                                         "bid_qty", "ask_px", "ask_qty").sort("seq").collect())
    ev = scan("session_event").sort("seq").collect()
    tr = scan("trade_execution").sort("seq").collect()
    return book, ev, tr


def run(bot_mod, seat, W, week, clock: _Clock, speed: float, capital: float, latency: int = 1):
    book, ev, trades = load(W, week)
    msgs_at, ts_at = defaultdict(list), {}
    for seq, t, ts, sym, mid, ref, bp_, bq, ap, aq in book.iter_rows():
        msgs_at[t].append((seq, ts, "book", (sym, mid, ref, list(zip(bp_, bq)), list(zip(ap, aq)))))
        ts_at.setdefault(t, ts)
    for r in ev.iter_rows(named=True):
        msgs_at[r["t"]].append((r["seq"], r["ts"], "event", r))
        ts_at[r["t"]] = min(ts_at.get(r["t"], r["ts"]), r["ts"])
    for t in msgs_at:
        msgs_at[t].sort(key=lambda m: m[0])
    last_ts_ = {t: m[-1][1] for t, m in msgs_at.items()}
    last_mid = {}
    for r in book.group_by("symbol").agg(pl.col("mid").last()).iter_rows():
        last_mid[r[0]] = r[1]

    owner = bot_mod.MyTrader()
    mk = Market()
    pf = None
    pending: dict[int, list] = defaultdict(list)
    fills, rejects, sent = [], defaultdict(int), []
    ssr: set[str] = set()
    t0w, ts0 = _real_time.monotonic(), ts_at[min(ts_at)]
    last_ts = last_ts_
    ticks = range(min(msgs_at), max(msgs_at) + 1)

    def set_clock(t, ts=None):
        if clock.mode == "tick":
            clock.now = t * 1.0
        else: 
            clock.now = ts if ts is not None else last_ts.get(t, clock.now)

    def send(sig, t):
        if sig is None:
            return
        sig = owner._gate(sig, mk, pf)
        if sig is None:
            return
        owner._note_sent(sig)
        sent.append((t, sig.symbol, sig.side, sig.quantity, sig.price))
        if latency == 0:
            execute(sig, t, clock.now if clock.mode != "wall" else None)
        else:
            pending[t + latency].append(sig)

    def execute(sig, t, ts):
        b = mk.books.get(sig.symbol)
        s = sig.symbol
        pos = pf.positions.get(s, 0)
        if sig.side == "sell" and s in ssr and sig.quantity > max(pos, 0):
            rejects["SSR_RESTRICTED"] += 1
            set_clock(t, ts)
            owner.on_reject("SSR_RESTRICTED", f"{s} short-sale restricted")
            return
        if b is None:
            return
        want = sig.quantity
        if sig.side == "buy":
            want = min(want, pf.limit - pos)
        else:
            want = min(want, pos + (pf.limit if pf.shorts else 0))
        levels = b[2] if sig.side == "buy" else b[1]
        for px, qty in levels:
            if want <= 0:
                break
            if (sig.side == "buy" and px > sig.price) or (sig.side == "sell" and px < sig.price):
                break
            q = int(min(want, qty))
            if q <= 0:
                continue
            sg = 1 if sig.side == "buy" else -1
            fee = px * q * FEE
            pf.cash -= sg * px * q + fee
            pf.total_fees_paid += fee
            pf.positions[s] = pf.positions.get(s, 0) + sg * q
            fills.append((t, s, sig.side, px, q))
            want -= q
            set_clock(t, ts)
            owner.on_fill(sig.side, s, q, px)

    opened = closed = False
    for t in ticks:
        if clock.mode == "wall" and math.isfinite(speed) and t in ts_at:
            wait = (ts_at[t] - ts0) / speed - (_real_time.monotonic() - t0w)
            if wait > 0:
                _real_time.sleep(wait)
        due = defaultdict(list)
        for sig in pending.pop(t, []):
            due[sig.symbol].append(sig)
        for seq, ts, kind, m in msgs_at.get(t, []):
            if kind == "book":
                sym, mid, ref, bids, asks = m
                mk.books[sym] = (mid, bids, asks)
                if ref and ref > 0:
                    owner._ref[sym] = ref
                for sig in due.pop(sym, []):                     
                    execute(sig, t, ts)
                continue
            e = m
            set_clock(t, ts)
            data = json.loads(e["data"]) if e["data"] else {}
            if e["event"] == "SESSION_OPEN":
                sc = data.get("scenario") or {}
                pf = Portfolio(capital, int(sc.get("position_limit") or 400),
                               bool((sc.get("flags") or {}).get("shorts_allowed")))
                for s_ in data.get("symbols") or []:
                    if s_ != "ARENA10":
                        pf.positions[s_] = GRANTED
            if e["event"] == "SESSION_CLOSED":
                closed = True
            if e["event"] == "SSR_ON" and data.get("symbol"):
                ssr.add(data["symbol"])
            owner.on_event(e["event"], e["message"], data)
            if e["event"] == "SESSION_OPEN":
                owner._preopen = False
                owner.on_open(data)
                owner._starter = bot_mod.new_starter(bot_mod.STARTER_SYMBOL)
                opened = True
            if (e["event"] in ("CALENDAR_EVENT", "SHOCK") and owner._shocks and opened):
                send(owner.on_tick(mk, pf), t)         
        for sigs in due.values():                     
            for sig in sigs:
                execute(sig, t, ts_at.get(t))
        for sym, b in mk.books.items():
            mk.hist[sym].append(b[0])
        if not opened or closed:
            continue
        set_clock(t)
        send(owner.on_tick(mk, pf), t)

    cash = sum((-1 if s == "buy" else 1) * px * q - px * q * FEE for _, _, s, px, q in fills)
    net = defaultdict(int)
    for _, sym, s, px, q in fills:
        net[sym] += q if s == "buy" else -q
    pnl = cash + sum(n * last_mid[s] for s, n in net.items())
    live = [r for r in trades.iter_rows(named=True)
            if seat in (r["taker_id"], r["maker_id"])]
    return dict(fills=fills, sent=sent, rejects=dict(rejects), pnl=pnl,
                fees=sum(px * q * FEE for _, _, _, px, q in fills),
                digest=hashlib.sha256(repr(fills).encode()).hexdigest()[:12],
                live=live, last_mid=last_mid, net=dict(net),
                wall=_real_time.monotonic() - t0w)


def by_symbol(fills, last_mid):
    """symbol -> (shares bought, avg buy, shares sold, avg sell, trading P&L incl. fees)."""
    out = {}
    for sym in sorted({f[1] for f in fills}):
        b = [(px, q) for _, s, sd, px, q in fills if s == sym and sd == "buy"]
        a = [(px, q) for _, s, sd, px, q in fills if s == sym and sd == "sell"]
        qb, qa = sum(q for _, q in b), sum(q for _, q in a)
        cash = sum(px * q for px, q in a) - sum(px * q for px, q in b) \
            - FEE * sum(px * q for px, q in a + b)
        out[sym] = (qb, sum(px * q for px, q in b) / qb if qb else 0.0,
                    qa, sum(px * q for px, q in a) / qa if qa else 0.0,
                    cash + (qb - qa) * last_mid[sym])
    return out


def summary(fills):
    out = {}
    for side in ("buy", "sell"):
        f = [x for x in fills if x[2] == side]
        q = sum(x[4] for x in f)
        out[side] = (len(f), q, sum(x[3] * x[4] for x in f) / q if q else None)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--bot", required=True)
    ap.add_argument("--warehouse", default="stores/arena.parquet")
    ap.add_argument("--week", type=int, required=True)
    ap.add_argument("--seat", default="stt_11_trader_1")
    ap.add_argument("--clock", choices=["tick", "tape", "wall"], default="tape")
    ap.add_argument("--speed", type=float, default=math.inf)
    ap.add_argument("--capital", type=float, default=300_000.0)
    ap.add_argument("--latency", type=int, default=1)
    ap.add_argument("--show-fills", action="store_true")
    a = ap.parse_args()
    clock = _Clock(a.clock)
    bot = load_bot(a.bot, a.seat, a.capital, clock)
    r = run(bot, a.seat, a.warehouse, a.week, clock, a.speed, a.capital, a.latency)
    s = summary(r["fills"])
    sp = "" if a.clock != "wall" else f" @ {a.speed:g}x"
    print(f"REPLAY week {a.week} · seat {a.seat} · clock={a.clock}{sp} · latency {a.latency}: "
          f"{len(r['fills'])} fills · {len(r['sent'])} orders · fees ${r['fees']:,.2f} · "
          f"trading P&L ${r['pnl']:+,.2f} · digest {r['digest']} · wall {r['wall']:.1f} s")
    for side, (n, q, avg) in s.items():
        if n:
            print(f"   {side:<4} {n:>3} fills · {q:>5} shares · avg ${avg:,.2f}")
    if r["rejects"]:
        print("   rejects: " + ", ".join(f"{k} {v}" for k, v in r["rejects"].items()))
    print("   net shares by symbol: " + ", ".join(f"{k} {v:+d}" for k, v in sorted(r["net"].items())))
    live = [(r["t"], r["symbol"], "buy" if r["buyer_id"] == a.seat else "sell", r["price"],
             r["quantity"]) for r in r["live"]]
    print(f"\n   {'symbol':<7} {'':<7} {'bought':>7} {'avg':>9} {'sold':>6} {'avg':>9} {'P&L $':>10}")
    for name, f in (("replay", r["fills"]), ("LIVE", live)):
        bs = by_symbol(f, r["last_mid"])
        for sym, (qb, ab, qa, aa, p) in bs.items():
            print(f"   {sym:<7} {name:<7} {qb:>7} {ab:>9.2f} {qa:>6} {aa:>9.2f} {p:>+10.2f}")
        print(f"   {'all':<7} {name:<7} {len(f):>4} fills {'':>21} {sum(v[4] for v in bs.values()):>+10.2f}")
    if a.show_fills:
        for f in r["fills"]:
            print("   fill", f)
        for o in r["sent"]:
            print("   order", o)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
