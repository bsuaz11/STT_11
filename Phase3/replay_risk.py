#!/usr/bin/env python
"""
Phase3/replay_risk.py — Session 6 (Fri Nov 6) through the risk lens, off scripts/replay_session.py.

1. Serves the recording with the course's replay server over a real local
   socket and captures the wire exactly as the dashboard REPLAY tab sees it.
   Each captured message is matched to its recorded tick (the server sends
   the file's non-audit records in order; the match is checked, not assumed).
2. LIVE: what our seats actually did through the scenario events — orders
   into halted symbols, how soon they traded again after each resume, the
   largest positions against the limit, liquidations, the VLTA bookbuild.
3. RISK LAYER ON THE SAME TAPE: our seats' real fills and the real marks fed,
   tick by tick, to today's team/risk.py RiskManager — which guard would have
   fired, when, and why, and how long the resume gate would have held each
   symbol on the real post-halt books.

    PYTHONPATH=engine:. python Phase3/replay_risk.py sessions/session_week06_20261106_214023_market.jsonl.gz

Writes Phase3/results/replay_session6_risk.txt (and prints it).
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import pathlib
import subprocess
import sys
import tempfile
from collections import defaultdict

import websockets

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT), str(ROOT / "engine")]

from Phase2.replay_wire_check import plain_copy
from team.config import CAPITAL
from team.risk import TRADER_LIMITS, RiskManager

PREFIX = "stt_11_"
QUIET_SEC = 8.0
OUT = ROOT / "Phase3" / "results" / "replay_session6_risk.txt"
POSITION_LIMIT = 1000


async def capture(port: int) -> list[dict]:
    """Everything the replay server puts on the wire, in order."""
    for _ in range(500):
        try:
            ws = await websockets.connect(f"ws://localhost:{port}", max_size=None)
            break
        except OSError:
            await asyncio.sleep(0.02)
    else:
        raise SystemExit("replay server did not come up")
    out = []
    async with ws:
        while True:
            try:
                out.append(json.loads(await asyncio.wait_for(ws.recv(), QUIET_SEC)))
            except asyncio.TimeoutError:
                return out


def recorded(path: str) -> list[tuple[int, dict]]:
    """(tick, msg) for every record replay_session.py serves (non-aux)."""
    opener = gzip.open if path.endswith(".gz") else open
    rows = []
    with opener(path, "rt") as f:
        for line in f:
            if line.strip():
                r = json.loads(line)
                if not r.get("aux"):
                    rows.append((int(r["tick"]), r["msg"]))
    return rows


def ours(msg: dict) -> list[tuple[str, str]]:
    """(seat, side) for each of our seats in a trade."""
    return [(msg[k], "buy" if k == "buyer_id" else "sell")
            for k in ("buyer_id", "seller_id") if str(msg.get(k, "")).startswith(PREFIX)]


def live_report(stream: list[tuple[int, dict]], lines: list[str]) -> dict:
    """What our seats did on the day."""
    halted: dict[str, int] = {}
    windows: list[tuple[str, int, int]] = []
    market_halt = None
    into_halt: list[str] = []
    pos: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    peak: dict[str, tuple[int, str, int]] = {}
    events = []
    first_after: dict[tuple[str, int], int | None] = {}
    for tick, m in stream:
        if m["type"] == "session_event":
            ev, d = m["event"], m.get("data") or {}
            sym = d.get("symbol")
            if ev == "TRADING_HALT" and sym:
                halted[sym] = tick
            elif ev == "TRADING_RESUMED" and sym and sym in halted:
                windows.append((sym, halted.pop(sym), tick))
                first_after[(sym, tick)] = None
            elif ev == "MARKET_HALT":
                market_halt = tick
            elif ev == "MARKET_RESUMED":
                market_halt = None
            if ev in ("TRADING_HALT", "TRADING_RESUMED", "MARKET_HALT", "MARKET_RESUMED",
                      "CALENDAR_EVENT", "IPO_OPEN", "IPO_PRICED", "IPO_LISTED", "LIQUIDATION",
                      "SSR_ON"):
                events.append((tick, ev, sym or "", m.get("message", "")[:80]))
        elif m["type"] == "trade_execution":
            for seat, side in ours(m):
                sym = m["symbol"]
                if sym in halted or market_halt is not None:
                    start = halted.get(sym, market_halt)
                    into_halt.append(f"{seat[len(PREFIX):]} {side} {m['quantity']} {sym} "
                                     f"{tick - start} ticks into the halt (t{start})")
                pos[seat][sym] += m["quantity"] if side == "buy" else -m["quantity"]
                if abs(pos[seat][sym]) > peak.get(seat, (0, "", 0))[0]:
                    peak[seat] = (abs(pos[seat][sym]), sym, tick)
                for (rsym, rt), first in first_after.items():
                    if rsym == sym and first is None and tick >= rt and seat.startswith(PREFIX + "trader"):
                        first_after[(rsym, rt)] = tick - rt
    lines.append("LIVE — what our seats did on Nov 6 (the week-6 bot, before this phase)")
    lines.append(f"  scenario events on the wire: {len(events)}")
    for e in events:
        lines.append(f"    t{e[0]}  {e[1]:16s} {e[2]:6s} {e[3]}")
    lines.append(f"  liquidations of our seats: "
                 f"{sum(1 for e in events if e[1] == 'LIQUIDATION' and PREFIX in e[3]) or 'none'}")
    lines.append(f"  fills in a symbol while it was halted: {len(into_halt) or 'none (0)'}")
    lines.extend(f"    {x} — an IOC already in flight when the halt hit" for x in into_halt)
    lines.append(f"  largest position from fills per seat (limit {POSITION_LIMIT}, "
                 f"IPO allocation excluded): " + ", ".join(
                     f"{s[len(PREFIX):]} {q} {sym} (t{t})" for s, (q, sym, t) in sorted(peak.items())))
    lines.append("  halts and our first trader fill after each resume (live had no settle rule):")
    for sym, h, r in windows:
        f = first_after.get((sym, r))
        lines.append(f"    {sym:5s} halted t{h} resumed t{r} ({r - h} ticks): "
                     f"first trader fill {'+' + str(f) + ' ticks' if f is not None else 'none'}")
    return {"windows": windows}


def risk_replay(stream: list[tuple[int, dict]], seat: str, lines: list[str]) -> dict:
    """Feed the seat's real fills and the real marks to today's RiskManager."""
    with tempfile.TemporaryDirectory() as kd:
        r = RiskManager(seat, TRADER_LIMITS, float(CAPITAL.get(seat, 100_000.0)),
                        kill_dir=pathlib.Path(kd))
        cash = float(CAPITAL.get(seat, 100_000.0))
        pos: dict[str, int] = defaultdict(int)
        marks: dict[str, float] = {}
        offer: dict[str, float] = {}
        halted: set[str] = set()
        # The IPO allocation is not a trade on the wire. Infer it from the seat's
        # VLTA fills: it ended flat, so allocation = net shares it sold.
        alloc = defaultdict(int)
        for _, m in stream:
            if m["type"] == "trade_execution":
                for s, side in ours(m):
                    if s == seat and m["symbol"] == "VLTA":
                        alloc["VLTA"] += m["quantity"] if side == "sell" else -m["quantity"]
        log: list[str] = []
        worst = (0.0, 0)
        last_tick = None
        resume_ok: dict[tuple[str, int], int] = {}     # (symbol, resume tick) -> allowed at
        resumed_at: dict[str, int] = {}
        stopped_at: dict[str, int] = {}
        pnl_at_pricing = None
        for tick, m in stream:
            t = m["type"]
            if t == "book_snapshot":
                if m.get("ref_price"):
                    marks[m["symbol"]] = float(m["ref_price"])
                r.on_data(tick)
                bids, asks = m.get("bids") or [], m.get("asks") or []
                sym = m["symbol"]
                if sym in r.resume._resumed and sym not in halted:
                    why = r.resume.blocks(sym, tick, bids[0][0] if bids else None,
                                          asks[0][0] if asks else None)
                    if why is None:
                        resume_ok[(sym, resumed_at[sym])] = tick
            elif t == "session_event":
                ev, d = m["event"], m.get("data") or {}
                sym = d.get("symbol")
                if ev == "TRADING_HALT" and sym:
                    halted.add(sym)
                elif ev == "TRADING_RESUMED" and sym:
                    halted.discard(sym)
                    r.on_resume(sym, tick)
                    resumed_at[sym] = tick
                elif ev == "IPO_PRICED" and sym and d.get("offer_price"):
                    offer[sym] = float(d["offer_price"])
                    q = alloc.get(sym, 0)
                    if q > 0:
                        pos[sym] += q
                        cash -= q * offer[sym]
                        log.append(f"    t{tick}  IPO allocation (inferred) {q} {sym} @ {offer[sym]:.2f}")
            elif t == "trade_execution":
                for s, side in ours(m):
                    if s != seat:
                        continue
                    q, px = m["quantity"], m["price"]
                    pos[m["symbol"]] += q if side == "buy" else -q
                    cash += -q * px if side == "buy" else q * px
                    if m.get("taker_id") == seat:
                        cash -= float(m.get("fee") or 0.0)
                    elif m.get("maker_id") == seat:
                        cash += float(m.get("maker_rebate") or 0.0)
                    r.on_fill(m["symbol"], side, q, px)
            if tick != last_tick:                  # one risk pass per tick, like on_tick
                last_tick = tick
                vmarks = dict(marks)
                for sym, px in offer.items():          # unlisted IPO: at the offer
                    vmarks.setdefault(sym, px)
                equity = cash + sum(q * vmarks.get(s, 0.0) for s, q in pos.items() if q)
                before = r.kill.reasons()
                d = r.update(tick, equity, dict(pos), vmarks)
                if r.session.baseline is not None and r.session.pnl < worst[0]:
                    worst = (r.session.pnl, tick)
                if offer and pnl_at_pricing is None and r.session.baseline is not None:
                    pnl_at_pricing = (tick, r.session.pnl)
                for sym in d.flatten:                  # once per cool-down, as the seat acts
                    if tick - stopped_at.get(sym, -10**9) >= TRADER_LIMITS.stop_cooldown_ticks:
                        stopped_at[sym] = tick
                        e = r.book.entry(sym) or 0.0
                        log.append(f"    t{tick}  POSITION STOP {sym}: {pos[sym]:+d} entry {e:.2f} "
                                   f"mark {vmarks.get(sym, 0):.2f} ({vmarks.get(sym, 0) / e - 1:+.1%})"
                                   f" → flatten, no new {sym} for "
                                   f"{TRADER_LIMITS.stop_cooldown_ticks:.0f} ticks")
                for reason in set(r.kill.reasons()) - set(before):
                    log.append(f"    t{tick}  KILL SWITCH [{reason}] session P&L {r.session.pnl:+.2f}")
        lines.append(f"  {seat[len(PREFIX):]}")
        lines.append(f"    worst session P&L {worst[0]:+.2f} at t{worst[1]} "
                     f"(soft stop −{TRADER_LIMITS.soft_stop_pct * 1e5:,.0f}, "
                     f"hard −{TRADER_LIMITS.hard_stop_pct * 1e5:,.0f})")
        lines.append(f"    soft stop: {'FIRED' if r.session.soft else 'no'}   "
                     f"hard stop: {'FIRED' if r.session.hard else 'no'}   "
                     f"kill switch trips: {[h[2] for h in r.kill.history if h[1] == 'trip'] or 'none'}")
        if pnl_at_pricing:
            lines.append(f"    session P&L when the IPO priced (t{pnl_at_pricing[0]}, allocation "
                         f"valued at the offer until it lists): {pnl_at_pricing[1]:+.2f}")
        lines.extend(log or ["    no guard fired"])
        return {"resume_ok": resume_ok, "soft": r.session.soft, "hard": r.session.hard}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("recording")
    ap.add_argument("--speed", type=float, default=60.0)
    ap.add_argument("--port", type=int, default=8799)
    args = ap.parse_args()

    with tempfile.TemporaryDirectory() as folder:
        plain = plain_copy(args.recording, folder)
        server = subprocess.Popen(
            [sys.executable, str(ROOT / "scripts" / "replay_session.py"), plain,
             "--speed", str(args.speed), "--port", str(args.port)],
            stdout=subprocess.DEVNULL, stdin=subprocess.DEVNULL)
        try:
            wire = asyncio.run(capture(args.port))
        finally:
            server.terminate()
            server.wait()

    rec = recorded(args.recording)
    # The server streams from the moment it starts, so the listener joins a
    # little way in: the wire is a suffix of the recording. Find where.
    skip = next((k for k in range(len(rec) - len(wire) + 1)
                 if rec[k][1] == wire[0] and rec[k + len(wire) - 1][1] == wire[-1]), None)
    if skip is None or any(a != b for (_, a), b in zip(rec[skip:], wire, strict=True)):
        raise SystemExit(f"wire/recording mismatch: {len(wire)} on the wire, {len(rec)} recorded")
    stream = [(tick, msg) for (tick, _), msg in zip(rec[skip:], wire, strict=True)]
    t0 = rec[0][0]
    stream = [(t - t0, m) for t, m in stream]            # ticks from the start of the tape

    lines = [f"Session 6 (Fri Nov 6) — {pathlib.Path(args.recording).name}",
             (f"played through scripts/replay_session.py at ×{args.speed:g}: {len(wire)} messages "
             f"captured off the socket, every one identical to the recording from record "
             f"{skip} on (the {skip} before it went out while the listener connected: "
             f"t0–t{rec[skip][0] - t0}). Ticks count from the tape's first record "
             f"(t0 = venue tick {t0})."), ""]
    live = live_report(stream, lines)
    lines += ["", ("RISK LAYER ON THE SAME TAPE — our seats' real fills and the real marks, "
              "fed to today's team/risk.py")]
    seats = sorted({s for _, m in stream if m["type"] == "trade_execution"
                    for s, _ in ours(m) if s.startswith(PREFIX + "trader")})
    res = {}
    for seat in seats:
        res[seat] = risk_replay(stream, seat, lines)
    lines += ["", ("  resume gate on the real books: ticks until new exposure was allowed "
              "(settle 3 ticks + two-sided book ≤100 bps)")]
    ok = res[seats[0]]["resume_ok"] if seats else {}
    for sym, _h, rt in live["windows"]:
        allowed = ok.get((sym, rt))
        lines.append(f"    {sym:5s} resumed t{rt}: re-entry allowed "
                     f"{'+' + str(allowed - rt) + ' ticks' if allowed is not None and allowed >= rt else 'n/a'}")
    text = "\n".join(lines)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
