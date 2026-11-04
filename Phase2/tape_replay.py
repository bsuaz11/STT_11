#!/usr/bin/env python
"""
Phase2/tape_replay.py — the same signal against recorded real order books.

The simulator's market makers are caricatures. A recording is what the real
book did, so this is the harder test, at the price of having very few prints.

Rules of the replay
-------------------
* The signal sees a print at its tick and may act `latency` ticks later.
* An IOC sweeps the recorded depth at that tick, up to its limit price, and
  pays the taker fee. Depth already consumed at a price is not available again.
* No market impact, and nobody else races for the same quotes, so a fill here
  is an upper bound on what a live order would get.
* Shorts obey the tape's rules: none in a week that locked them, none after
  SSR_ON. Nothing trades in a halted symbol. The hold-out caveat is in BACKTEST.md: these tapes helped form the
  hypothesis, so they are a realism check, not a clean out-of-sample test.

    PYTHONPATH=engine:. python Phase2/tape_replay.py homework4/session_week04_*.jsonl.gz
"""

from __future__ import annotations

import argparse
import gzip
import json
import pathlib
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from Phase2.event_gap import EventGapSignal, GapParams  # noqa: E402

SEAT_CAP = 415              # team/trader.py's seat cap at limit 1000
SEAT_CASH = 100_000.0
MAX_GROSS = 1.5
SKIP = ("ARENA10",)
PRINT_KINDS = ("earnings", "econ_print")


@dataclass
class Tape:
    """A recording reduced to what the replay needs."""

    books: dict[int, dict[str, dict]]       # tick -> symbol -> last snapshot of that tick
    prints: dict[int, list[dict]]           # tick -> CALENDAR_EVENT payloads
    ssr: dict[str, int]                     # symbol -> tick SSR tripped
    halts: list[tuple[int, str]]
    resumes: list[tuple[int, str]]
    shorts_allowed: bool
    first_tick: int
    last_tick: int


def iter_records(path: str):
    """Yield every record with a `tick`. Class tapes carry one; a raw exchange
    recording has only `ts`, so there it comes from the last tick the venue
    reported plus the seconds since (one tick per second)."""
    opener = gzip.open if path.endswith(".gz") else open
    anchor: tuple[int, float] | None = None
    with opener(path, "rt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            msg = rec.get("msg") or {}
            reported = msg.get("tick") if msg.get("type") == "leaderboard" else (msg.get("data") or {}).get("tick")
            if isinstance(reported, int) and "ts" in rec:
                anchor = (reported, float(rec["ts"]))
            if not isinstance(rec.get("tick"), int):
                if anchor is None or "ts" not in rec:
                    rec["tick"] = None
                else:
                    rec["tick"] = anchor[0] + int(float(rec["ts"]) - anchor[1])
            yield rec


def load_tape(path: str) -> Tape:
    books: dict[int, dict[str, dict]] = defaultdict(dict)
    prints: dict[int, list[dict]] = defaultdict(list)
    ssr: dict[str, int] = {}
    halts: list[tuple[int, str]] = []
    resumes: list[tuple[int, str]] = []
    shorts = "week02" not in path           # week 2 locked shorts; its tape predates the flag
    first, last = None, 0
    for rec in iter_records(path):
        msg, tick = rec.get("msg") or {}, rec["tick"]
        if tick is None:
            continue
        if msg.get("type") == "book_snapshot":
            books[tick][msg["symbol"]] = {"bids": msg["bids"], "asks": msg["asks"],
                                          "ref": msg.get("ref_price") or msg["mid_price"]}
            first = tick if first is None else first
            last = tick
        elif msg.get("type") == "session_event":
            data = msg.get("data") or {}
            if msg["event"] == "CALENDAR_EVENT" and data.get("kind") in PRINT_KINDS:
                prints[tick].append(data)
            elif msg["event"] == "SSR_ON" and data.get("symbol"):
                ssr.setdefault(data["symbol"], tick)
            elif msg["event"] == "TRADING_HALT" and data.get("symbol"):
                halts.append((tick, data["symbol"]))
            elif msg["event"] == "TRADING_RESUMED" and data.get("symbol"):
                resumes.append((tick, data["symbol"]))
            elif msg["event"] == "SESSION_OPEN":
                flags = (data.get("scenario") or {}).get("flags") or {}
                if "shorts_allowed" in flags and data.get("week"):
                    shorts = bool(flags["shorts_allowed"])
    return Tape(books, prints, ssr, halts, resumes, shorts, first or 0, last)


def replay(tape: Tape, params: GapParams, latency: int, fee: float) -> dict:
    """Run the signal over the tape. One order per symbol per tick."""
    signal = EventGapSignal(params=params, taker_fee=fee)
    positions: dict[str, int] = defaultdict(int)
    cash = SEAT_CASH
    consumed: dict[tuple[str, str, float], float] = defaultdict(float)
    marks: dict[str, float] = {}
    last_book: dict[str, dict] = {}
    blind: dict[str, int] = {}
    halted: set[str] = set()
    halt_events: dict[int, list[tuple[str, bool]]] = defaultdict(list)
    for at, sym in tape.halts:
        halt_events[at].append((sym, True))
    for at, sym in tape.resumes:
        halt_events[at].append((sym, False))
    fills = 0
    missed_no_depth = 0

    def sweep(symbol: str, side: str, qty: int, limit: float, levels: list) -> tuple[int, float]:
        nonlocal cash, fills
        got, spent = 0, 0.0
        for price, size in levels:
            if (side == "buy" and price > limit) or (side == "sell" and price < limit):
                break
            key = (symbol, side, price)
            take = min(qty - got, int(max(0.0, size - consumed[key])))
            if take <= 0:
                continue
            consumed[key] += take
            got += take
            spent += take * price
            signal.on_fill(symbol, side, take, price)
            fills += 1
            if got >= qty:
                break
        signed = spent if side == "sell" else -spent
        cash += signed - spent * fee
        positions[symbol] += got if side == "buy" else -got
        return got, spent

    for tick in range(tape.first_tick, tape.last_tick + 1):
        for sym, is_halt in halt_events.get(tick, []):
            (halted.add if is_halt else halted.discard)(sym)
        for ev in tape.prints.get(tick, []):
            for sym in ev.get("affected") or []:
                # Only marks from BEFORE this tick: on_print gets the pre-print value.
                if sym not in SKIP and marks.get(sym):
                    signal.on_print(sym, float(ev["pct"]), marks[sym], tick, positions[sym])
                    blind[sym] = tick + latency
        for sym, book in tape.books.get(tick, {}).items():
            last_book[sym] = book
        for sym in list(signal.plans):
            book = last_book.get(sym)
            if book is None or tick < blind.get(sym, 0) or sym in halted:
                continue
            bid = book["bids"][0][0] if book["bids"] else None
            ask = book["asks"][0][0] if book["asks"] else None
            ref = book["ref"] or marks.get(sym) or 0.0
            pos = positions[sym]
            gross = sum(abs(q) * marks.get(s, 0.0) for s, q in positions.items())
            equity = cash + sum(q * marks.get(s, 0.0) for s, q in positions.items())
            spare = max(0, int((MAX_GROSS * equity - gross) / ref)) if ref else 0
            long_room = max(0, min(SEAT_CAP - pos, spare, int(cash * 0.9 / ask) if ask else 0))
            may_short = tape.shorts_allowed and tick < tape.ssr.get(sym, 1 << 62)
            short_room = max(0, min(pos + SEAT_CAP, spare)) if may_short else max(0, pos)
            order = signal.decide(sym, tick, bid, ask, pos, long_room, short_room)
            if order is None:
                continue
            levels = book["asks"] if order.side == "buy" else book["bids"]
            got, _ = sweep(sym, order.side, order.quantity, order.price, levels)
            if got == 0:
                missed_no_depth += 1
        for sym, book in tape.books.get(tick, {}).items():
            if book["ref"]:
                marks[sym] = book["ref"]

    # A plan the tape ends on (should not happen: the time stop is 30 ticks).
    rows = [asdict(r) | {"traded": r.traded} for r in signal.closed]
    return {"plans": rows, "open_plans": list(signal.plans), "fills": fills,
            "unfilled_orders": missed_no_depth,
            "pnl": sum(r["pnl"] for r in rows), "fees": sum(r["fees"] for r in rows),
            "traded": sum(r["traded"] for r in rows), "prints": len(rows)}


def main() -> None:
    ap = argparse.ArgumentParser(description="Replay the signal on recorded order books")
    ap.add_argument("tapes", nargs="+")
    ap.add_argument("--buffer-bps", type=float, default=GapParams.buffer_bps)
    ap.add_argument("--target-mult", type=float, default=GapParams.target_mult)
    ap.add_argument("--max-entry", type=int, default=GapParams.max_entry_ticks)
    ap.add_argument("--fee", type=float, default=0.003, help="taker fee the tape was charged")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    params = GapParams(buffer_bps=args.buffer_bps, target_mult=args.target_mult,
                       max_entry_ticks=args.max_entry)
    report = {}
    for path in args.tapes:
        tape = load_tape(path)
        name = pathlib.Path(path).name
        report[name] = {"halts": len(tape.halts), "resumes": len(tape.resumes), "shorts_allowed": tape.shorts_allowed,
                        "prints": sum(len(v) for v in tape.prints.values())}
        print(f"\n{name}  shorts={tape.shorts_allowed}  halts={len(tape.halts)}")
        for latency in (0, 1, 2, 3, 5):
            r = replay(tape, params, latency, args.fee)
            report[name][f"latency_{latency}"] = r
            print(f"  latency {latency}: pnl {r['pnl']:+9.2f}  fees {r['fees']:8.2f}  "
                  f"traded {r['traded']:2d}/{r['prints']:2d} plans  fills {r['fills']:3d}  "
                  f"unfilled orders {r['unfilled_orders']:2d}")
            if latency == 1:
                for p in r["plans"]:
                    if p["traded"]:
                        print(f"      {p['symbol']:6s} {p['pct'] * 100:+6.2f}%  bought {p['bought']:4d} "
                              f"sold {p['sold']:4d}  pnl {p['pnl']:+9.2f}  held {p['t1'] - p['t0']} ticks")
    if args.out:
        pathlib.Path(args.out).write_text(json.dumps(report, indent=1))


if __name__ == "__main__":
    main()
