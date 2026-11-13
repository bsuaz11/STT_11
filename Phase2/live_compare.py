#!/usr/bin/env python
"""
Phase2/live_compare.py — what the bots actually did in a recorded session.

    PYTHONPATH=engine:. python Phase2/live_compare.py Phase2/data/session_week06_*.jsonl.gz \\
        [--logs Phase2/logs/<run>] [--out Phase2/results/live_week06.json]

Reads a session recording (scripts/replay_session.py plays the same file) and
reports, for STT_11 only:

  Part A   liquidations, peak team position per symbol against the limit, what
           each seat did after each TRADING_RESUMED, the IPO bid, and uptime
  Part B   every print: what the seats traded, the realized P&L net of fees,
           and what tape_replay.py gets on the same book with the same signal

The gap between the last two is the live/offline reconciliation.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
from collections import defaultdict

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from Phase2.event_gap import GapParams  # noqa: E402
from Phase2.tape_replay import iter_records, load_tape, replay  # noqa: E402

PREFIX = "stt_11_"
SEATS = ("stt_11_trader_1", "stt_11_trader_2", "stt_11_broker_1")
TRADERS = ("stt_11_trader_1", "stt_11_trader_2")
PRINT_WINDOW = 400          # ticks after a print a fill can still belong to it
RESUME_WINDOW = 120         # ticks after a resume that count as "activity resumed"


def analyse(path: str) -> dict:
    """One pass over the recording, keeping only what concerns our seats."""
    limit = week = None
    first_tick = last_tick = None
    open_tick = None
    liquidations: list[dict] = []
    peak: dict[str, int] = defaultdict(int)
    peak_at: dict[str, int] = {}
    halts, resumes, prints, ipo_events = [], [], [], []
    trades: list[dict] = []
    rejects: dict[str, int] = defaultdict(int)
    acks_by_symbol: dict[str, list[int]] = defaultdict(list)
    ipo_bids: list[dict] = []
    seen_lb = 0
    present: dict[str, int] = defaultdict(int)
    has_connection_data = False
    equity: dict[str, list[tuple[int, float]]] = defaultdict(list)
    has_positions = False

    for rec in iter_records(path):
        msg, tick = rec.get("msg") or {}, rec.get("tick")
        kind = msg.get("type")
        if isinstance(tick, int):
            first_tick = tick if first_tick is None else first_tick
            last_tick = tick
        if kind == "session_event":
            event, data = msg.get("event"), msg.get("data") or {}
            if event == "SESSION_OPEN" and not data.get("late_join"):
                sc = data.get("scenario") or {}
                limit = sc.get("position_limit") or limit
                week = data.get("week", week)
                open_tick = open_tick if open_tick is not None else tick
            elif event == "LIQUIDATION" and str(data.get("team_id", "")).startswith(PREFIX):
                liquidations.append({"tick": tick, **data})
            elif event == "TRADING_HALT":
                halts.append({"tick": tick, "symbol": data.get("symbol"), "reason": data.get("reason")})
            elif event == "TRADING_RESUMED":
                resumes.append({"tick": tick, "symbol": data.get("symbol")})
            elif event == "MARKET_RESUMED":
                resumes.append({"tick": tick, "symbol": None})
            elif event == "CALENDAR_EVENT":
                prints.append({"tick": tick, "kind": data.get("kind"), "pct": data.get("pct"),
                               "affected": data.get("affected") or []})
            elif event and event.startswith("IPO_"):
                ipo_events.append({"tick": tick, "event": event, "message": msg.get("message")})
        elif kind == "trade_execution":
            buyer, seller = msg.get("buyer_id", ""), msg.get("seller_id", "")
            if buyer.startswith(PREFIX) or seller.startswith(PREFIX):
                trades.append({"tick": tick, "symbol": msg["symbol"], "price": msg["price"],
                               "quantity": msg["quantity"], "buyer": buyer, "seller": seller,
                               "taker": msg.get("taker_id"), "fee": msg.get("fee") or 0.0,
                               "rebate": msg.get("maker_rebate") or 0.0})
        elif kind == "leaderboard":
            seen_lb += 1
            team: dict[str, int] = defaultdict(int)
            for entry in list(msg.get("traders") or []) + list(msg.get("brokers") or []):
                tid = entry.get("team_id", "")
                if tid.startswith(PREFIX):
                    has_positions = True
                    equity[tid].append((tick, float(entry.get("net_worth", 0.0))))
                    for sym, qty in (entry.get("positions") or {}).items():
                        team[sym] += int(qty)
            for sym, qty in team.items():
                if abs(qty) > peak[sym]:
                    peak[sym], peak_at[sym] = abs(qty), tick
            clients = msg.get("connected_clients") or []
            if clients:
                has_connection_data = True
                for seat in SEATS:
                    present[seat] += seat in clients
        elif kind == "error" and str(msg.get("team_id") or rec.get("team_id") or "").startswith(PREFIX):
            rejects[msg.get("code", "?")] += 1
        elif kind == "ipo_subscribe" and str(msg.get("team_id", "")).startswith(PREFIX):
            ipo_bids.append({"tick": tick, "seat": msg["team_id"], "quantity": msg.get("quantity"),
                             "max_price": msg.get("max_price")})
        elif kind == "order_ack" and str(msg.get("team_id", "")).startswith(PREFIX):
            acks_by_symbol[msg.get("symbol", "")].append(tick)

    after_resume = []
    for r in resumes:
        if r["symbol"] is None:
            continue
        first = {"trader": None, "broker": None}
        for t in trades:
            delay = t["tick"] - r["tick"]
            if t["symbol"] == r["symbol"] and 0 <= delay <= RESUME_WINDOW:
                for seat in (t["buyer"], t["seller"]):
                    role = "trader" if seat in TRADERS else "broker" if seat.startswith(PREFIX) else None
                    if role and (first[role] is None or delay < first[role]):
                        first[role] = delay
        acks = [t - r["tick"] for t in acks_by_symbol.get(r["symbol"], [])
                if t is not None and 0 <= t - r["tick"] <= RESUME_WINDOW]
        after_resume.append({**r, "trader_first_fill": first["trader"],
                             "broker_first_fill": first["broker"],
                             "first_ack": min(acks) if acks else None})

    # Net change in the team's position from its own fills: the position check
    # for a recording with no leaderboard. It leaves out the granted shares.
    net: dict[str, int] = defaultdict(int)
    seat_net: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    fill_peak: dict[str, int] = defaultdict(int)
    for t in trades:
        for seat, sign in ((t["buyer"], 1), (t["seller"], -1)):
            if seat.startswith(PREFIX):
                net[t["symbol"]] += sign * t["quantity"]
                seat_net[seat][t["symbol"]] += sign * t["quantity"]
        fill_peak[t["symbol"]] = max(fill_peak[t["symbol"]], abs(net[t["symbol"]]))

    return {
        "week": week, "limit": limit, "ticks": (first_tick, last_tick), "open_tick": open_tick,
        "liquidations": liquidations,
        "peak_team_position": dict(sorted(peak.items(), key=lambda kv: -kv[1])),
        "peak_at": peak_at, "has_positions": has_positions,
        "peak_from_fills": dict(sorted(fill_peak.items(), key=lambda kv: -kv[1])),
        "end_position_from_fills": {k: dict(v) for k, v in seat_net.items()},
        "halts": halts, "resumes": after_resume, "prints": prints, "ipo_events": ipo_events,
        "ipo_bids": ipo_bids, "rejects": dict(rejects), "trades": trades,
        "uptime": ({s: present[s] / seen_lb for s in SEATS}
                   if has_connection_data and seen_lb else None),
        "equity": {k: v for k, v in equity.items()},
    }


def realized_by_print(a: dict) -> list[dict]:
    """Attribute our trader seats' fills to the latest print in that symbol.

    PRINT_WINDOW outlasts a 300-tick halt, because a position held through one
    only exits afterwards. Inventory still open is marked at its last fill, and
    fills with no print before them (probes, listing exits) are left out.
    """
    rows = {}
    for p in a["prints"]:
        for sym in p["affected"]:
            rows[(p["tick"], sym)] = {"tick": p["tick"], "symbol": sym, "pct": p["pct"], "fills": 0,
                                      "open_shares": 0, "cash": 0.0, "fees": 0.0, "last": 0.0}
    for t in a["trades"]:
        side = "buy" if t["buyer"] in TRADERS else "sell" if t["seller"] in TRADERS else None
        ticks = [tick for tick, sym in rows
                 if sym == t["symbol"] and tick <= t["tick"] <= tick + PRINT_WINDOW]
        if side is None or not ticks:
            continue
        row = rows[(max(ticks), t["symbol"])]
        signed = t["quantity"] if side == "buy" else -t["quantity"]
        row["open_shares"] += signed
        row["cash"] -= signed * t["price"]
        row["fills"] += 1
        row["last"] = t["price"]
        if t["taker"] in TRADERS:
            row["fees"] += t["fee"]
    return [{"tick": r["tick"], "symbol": r["symbol"], "pct": r["pct"], "fills": r["fills"],
             "open_shares": r["open_shares"], "fees": r["fees"],
             "pnl": r["cash"] + r["open_shares"] * r["last"] - r["fees"]}
            for r in rows.values() if r["fills"]]


def trading_curve(path: str, a: dict) -> list[tuple[int, float]]:
    """Our trader seats' P&L from their own trades, marked at the venue mark,
    one point per tick since the open. Net of taker fees; granted shares are
    not traded, so this compares with the backtest's excess over a control.

    A market tape has no allocation records, so an IPO allocation is inferred
    as the shares a seat sold beyond what it bought, booked at the offer
    when the deal priced.
    """
    offer, priced_at = None, None
    net: dict[tuple[str, str], int] = defaultdict(int)
    for t in a["trades"]:
        for seat in TRADERS:
            if seat in (t["buyer"], t["seller"]):
                net[(seat, t["symbol"])] += t["quantity"] if t["buyer"] == seat else -t["quantity"]
    for e in a["ipo_events"]:
        if e["event"] == "IPO_PRICED":
            priced_at = e["tick"]
            m = re.search(r"priced at \$([\d.]+)", e["message"] or "")
            offer = float(m.group(1)) if m else None
    listed = {e["message"].split()[0] for e in a["ipo_events"] if e["event"] == "IPO_LISTED"}
    allocation = {key: -q for key, q in net.items() if q < 0 and key[1] in listed}

    cash, pos, mark = 0.0, defaultdict(int), {}
    trades = iter(a["trades"])
    pending = next(trades, None)
    curve: list[tuple[int, float]] = []
    start = a["open_tick"] if a["open_tick"] is not None else a["ticks"][0]
    last_tick = None
    for rec in iter_records(path):
        tick, msg = rec.get("tick"), rec.get("msg") or {}
        if tick is None:
            continue
        if msg.get("type") == "book_snapshot" and msg.get("ref_price"):
            mark[msg["symbol"]] = msg["ref_price"]
        if offer and priced_at is not None and tick >= priced_at and allocation:
            for (_seat, sym), q in allocation.items():
                pos[sym] += q
                cash -= q * offer
                mark.setdefault(sym, offer)       # unlisted until it trades: worth the offer
            allocation = {}
        while pending is not None and pending["tick"] <= tick:
            for seat in TRADERS:
                if seat in (pending["buyer"], pending["seller"]):
                    signed = pending["quantity"] if pending["buyer"] == seat else -pending["quantity"]
                    pos[pending["symbol"]] += signed
                    cash -= signed * pending["price"]
                    if pending["taker"] == seat:
                        cash -= pending["fee"]
            pending = next(trades, None)
        if tick != last_tick:
            curve.append((tick - start, cash + sum(q * mark.get(s, 0.0) for s, q in pos.items())))
            last_tick = tick
    return curve


def parse_logs(logdir: str | None) -> dict:
    """Pull the seats' own account of the session out of their logs."""
    if not logdir:
        return {}
    out: dict[str, dict] = {}
    closed = re.compile(r"EVENTGAP closed (\S+) ([+-][\d.]+)%: bought (\d+) sold (\d+) pnl=([+-][\d.]+) fees=([\d.]+)")
    for path in sorted(pathlib.Path(logdir).glob("*.log")):
        plans, rejects, reconnects, gate_blocks, starts = [], defaultdict(int), 0, 0, 0
        ipo = []
        for line in path.read_text(errors="replace").splitlines():
            m = closed.search(line)
            if m:
                plans.append({"symbol": m[1], "pct": float(m[2]), "bought": int(m[3]),
                              "sold": int(m[4]), "pnl": float(m[5]), "fees": float(m[6])})
            elif "REJECT [" in line:
                rejects[line.split("REJECT [")[1].split("]")[0]] += 1
            elif "Exchange error [" in line:
                rejects[line.split("Exchange error [")[1].split("]")[0]] += 1
            elif "reconnecting in" in line:
                reconnects += 1
            elif "Gate blocked" in line:
                gate_blocks += 1
            elif "SUPERVISOR start" in line:
                starts += 1
            elif "IPO " in line and ("bidding" in line or "confirmed" in line or "resending" in line):
                ipo.append(line.strip())
        out[path.stem] = {"plans": plans, "rejects": dict(rejects), "reconnects": reconnects,
                          "gate_blocks": gate_blocks, "process_starts": starts, "ipo": ipo}
    return out


def report(a: dict, logs: dict, tape_pnl: dict | None) -> None:
    """Print Part A evidence and the per-print live P&L."""
    print(f"week {a['week']}  limit {a['limit']}  ticks {a['ticks']}\n")
    print("PART A")
    print(f"  forced liquidation of our seats: {a['liquidations'] or 'none'}")
    if a["has_positions"]:
        top = list(a["peak_team_position"].items())[:5]
        ok = a["limit"] is None or all(q <= a["limit"] for _, q in top)
        print(f"  peak team position per symbol (limit {a['limit']}): {top} -> "
              f"{'within limit' if ok else 'BREACH'}")
    else:
        print(f"  this recording has no leaderboard. Peak team position change from our fills "
              f"(limit {a['limit']}, granted shares excluded): {list(a['peak_from_fills'].items())[:5]}")
    print(f"  halts: {len(a['halts'])}, resumes: {len(a['resumes'])}")
    for r in a["resumes"]:
        print(f"    {r['symbol']:6s} resumed t{r['tick']}: trader fill +{r['trader_first_fill']}, "
              f"broker fill +{r['broker_first_fill']}, ack +{r['first_ack']} (ticks; None = not in window)")
    print(f"  IPO events: {[e['event'] for e in a['ipo_events']]}")
    print(f"  IPO bids in the recording: {a['ipo_bids'] or 'not recorded (see the logs below)'}")
    print(f"  uptime in leaderboards: {a['uptime'] if a['uptime'] else 'not in this recording'}")
    if a["rejects"]:
        print(f"  rejects: {a['rejects']}")
    for seat, s in logs.items():
        print(f"  log {seat}: starts={s['process_starts']} reconnects={s['reconnects']} "
              f"gate_blocks={s['gate_blocks']} rejects={s['rejects']}")
        for line in s["ipo"][:4]:
            print(f"      {line}")

    print("\nPART B: prints")
    rows = realized_by_print(a)
    for r in rows:
        print(f"  t{r['tick']} {r['symbol']:6s} {r['pct'] * 100:+6.2f}%  fills {r['fills']:3d}  "
              f"open {r['open_shares']:+5d}  pnl {r['pnl']:+9.2f}  fees {r['fees']:7.2f}")
    print(f"  live realized on prints: {sum(r['pnl'] for r in rows):+.2f} "
          f"(fees {sum(r['fees'] for r in rows):.2f})")
    logged = [p for s in logs.values() for p in s["plans"]]
    if logged:
        print(f"  seats' own plan ledger: {len(logged)} plans, {sum(p['pnl'] for p in logged):+.2f}")
    if tape_pnl:
        print(f"  tape replay on this recording: {tape_pnl}")


def main() -> None:
    """Analyse one recording (and the seats' logs, if any)."""
    ap = argparse.ArgumentParser(description="Report what the bots did in a recorded session")
    ap.add_argument("recording")
    ap.add_argument("--logs", default=None, help="a Phase2/logs/<run> directory")
    ap.add_argument("--out", default=None)
    ap.add_argument("--fee", type=float, default=0.004)
    ap.add_argument("--seats", type=int, default=2, help="trader seats that ran live")
    args = ap.parse_args()
    a = analyse(args.recording)
    logs = parse_logs(args.logs)
    tape = load_tape(args.recording)
    frozen = ROOT / "Phase2" / "results" / "backtest_results.json"
    params = GapParams()
    if frozen.exists():
        params = GapParams(**json.loads(frozen.read_text())["frozen_params"])
    tape_pnl = {f"latency_{lat}": round(replay(tape, params, lat, args.fee, args.seats)["pnl"], 2)
                for lat in (0, 1, 2, 3)}
    report(a, logs, tape_pnl)
    curve = trading_curve(args.recording, a)
    print(f"  trading P&L of the trader seats, marked at the close: {curve[-1][1]:+.2f}")
    if args.out:
        pathlib.Path(args.out).write_text(json.dumps(
            {"analysis": a, "by_print": realized_by_print(a), "logs": logs, "tape_replay": tape_pnl,
             "trading_curve": curve}))


if __name__ == "__main__":
    main()
