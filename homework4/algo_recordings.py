from __future__ import annotations

import gzip
import json
import os
import re
import statistics
from collections import defaultdict

# organize session recordings into column order per message type
COLUMNS: dict[str, list[str]] = {
    "book_snapshot": ["week", "seq", "tick", "t", "ts", "symbol",
                      "best_bid", "best_bid_qty", "best_ask", "best_ask_qty",
                      "mid", "spread", "microprice", "obi", "ref_price", "n_bid", "n_ask"],
    "book_level": ["week", "seq", "side", "lvl", "px", "qty"],
    "trade_execution": ["week", "seq", "tick", "t", "ts", "symbol", "trade_id", "price",
                        "quantity", "notional", "aggressor", "buyer_id", "seller_id",
                        "taker_id", "maker_id", "fee", "maker_rebate"],
    "order_ack": ["week", "seq", "tick", "t", "ts", "symbol", "order_id", "team_id", "side",
                  "price", "quantity", "queue_ahead", "level_qty"],
    "order_cancel": ["week", "seq", "tick", "t", "ts", "symbol", "order_id", "team_id", "fee"],
    "order_reject": ["week", "seq", "tick", "t", "ts", "team_id", "code", "message"],
    "session_event": ["week", "seq", "tick", "t", "ts", "event", "message", "kind", "symbol",
                      "pct", "ramp_ticks", "overshoot", "affected", "data"],
    "leaderboard": ["week", "seq", "tick", "t", "ts", "team_id", "role", "cash", "net_worth",
                    "realized_pnl", "unrealized_pnl", "fees_paid", "rebates", "trade_count"],
}

def open_tape(path: str):
    return gzip.open(path, "rt") if path.endswith(".gz") else open(path)

def guess_week(path: str) -> int:
    m = re.search(r"week(\d+)", os.path.basename(path))
    if not m:
        raise SystemExit(f"cannot tell the week from {path!r}: pass --week N")
    return int(m.group(1))


def jsonl_bytes(path: str) -> int:
    if not path.endswith(".gz"):
        return os.path.getsize(path)
    with open(path, "rb") as f:
        f.seek(-4, os.SEEK_END)
        return int.from_bytes(f.read(4), "little")


def event_clock_offset(path: str) -> tuple[int, str]:
    announced: dict[tuple, int] = {}
    offsets: list[int] = []
    open_tick = None
    with open_tape(path) as f:
        for line in f:
            if '"session_event"' not in line:
                continue
            rec = json.loads(line)
            m, d = rec["msg"], rec["msg"].get("data") or {}
            if m.get("event") == "SESSION_OPEN" and open_tick is None:
                open_tick = int(d.get("tick", rec["tick"]))
            elif m.get("event") == "CALENDAR":
                for e in d.get("imminent") or []:
                    announced[(e.get("kind"), e.get("symbol"))] = int(e["tick"])
            elif m.get("event") == "CALENDAR_EVENT":
                key = (d.get("kind"), d.get("symbol"))
                if key in announced:
                    offsets.append(int(d.get("tick", rec["tick"])) - announced[key])
    if offsets:
        return int(statistics.median(offsets)), f"CALENDAR vs CALENDAR_EVENT ({len(offsets)} events)"
    if open_tick is not None:
        return open_tick, "SESSION_OPEN tick"
    return 0, "no session_event: t = tick"

def _f(x):
    return None if x is None else float(x)

def read_tape(path: str, week: int) -> tuple[dict[str, dict[str, list]], dict]:
    t0, how = event_clock_offset(path)
    cols: dict[str, dict[str, list]] = {k: {c: [] for c in v} for k, v in COLUMNS.items()}
    depth: dict[str, list] = {"bid_px": [], "bid_qty": [], "ask_px": [], "ask_qty": []}
    seen: dict[str, int] = defaultdict(int)

    def put(table, row):
        c = cols[table]
        for k in COLUMNS[table]:
            c[k].append(row[k])

    with open_tape(path) as f:
        for seq, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            m = rec["msg"]
            kind = m.get("type")
            seen[kind] += 1
            tick = int(rec["tick"])
            base = {"week": week, "seq": seq, "tick": tick, "t": tick - t0, "ts": float(rec["ts"])}
            if kind == "book_snapshot":
                bids, asks = m.get("bids") or [], m.get("asks") or []
                put(kind, dict(base, symbol = m["symbol"],
                               best_bid = _f(bids[0][0]) if bids else None,
                               best_bid_qty = _f(bids[0][1]) if bids else None,
                               best_ask = _f(asks[0][0]) if asks else None,
                               best_ask_qty = _f(asks[0][1]) if asks else None,
                               mid = _f(m.get("mid_price")),
                               spread = (asks[0][0] - bids[0][0]) if (bids and asks) else None,
                               microprice = _f(m.get("microprice")), obi=_f(m.get("obi")),
                               ref_price = _f(m.get("ref_price")),
                               n_bid = len(bids), n_ask=len(asks)))
                depth["bid_px"].append([float(b[0]) for b in bids])
                depth["bid_qty"].append([float(b[1]) for b in bids])
                depth["ask_px"].append([float(a[0]) for a in asks])
                depth["ask_qty"].append([float(a[1]) for a in asks])
                for side, lv in (("B", bids), ("A", asks)):
                    for i, (px, q) in enumerate(lv):
                        put("book_level", {"week": week, "seq": seq, "side": side, "lvl": i,
                                           "px": float(px), "qty": float(q)})
            elif kind == "trade_execution":
                px, q = float(m["price"]), int(m["quantity"])
                put(kind, dict(base, symbol = m["symbol"], trade_id = m.get("trade_id"), price = px,
                               quantity = q, notional = px * q, aggressor = m.get("aggressor"),
                               buyer_id = m.get("buyer_id"), seller_id = m.get("seller_id"),
                               taker_id = m.get("taker_id"), maker_id = m.get("maker_id"),
                               fee = _f(m.get("fee")), maker_rebate = _f(m.get("maker_rebate"))))
            elif kind == "order_ack":
                put(kind, dict(base, symbol = m.get("symbol"), order_id = m.get("order_id"),
                               team_id = m.get("team_id"), side = m.get("side"),
                               price = _f(m.get("price")), quantity = m.get("quantity"),
                               queue_ahead = m.get("queue_ahead"), level_qty = m.get("level_qty")))
            elif kind == "order_cancel":
                put(kind, dict(base, symbol = m.get("symbol"), order_id = m.get("order_id"),
                               team_id = m.get("team_id"), fee = _f(m.get("fee"))))
            elif kind == "order_reject":
                put(kind, dict(base, team_id = m.get("team_id"), code = m.get("code"),
                               message = m.get("message")))
            elif kind == "session_event":
                d = m.get("data") or {}
                aff = d.get("affected")
                put(kind, dict(base, event = m.get("event"), message = m.get("message"),
                               kind = d.get("kind"), symbol = d.get("symbol"), pct = _f(d.get("pct")),
                               ramp_ticks = d.get("ramp_ticks"), overshoot = _f(d.get("overshoot")),
                               affected = ",".join(aff) if aff else None,
                               data = json.dumps(d, separators = (",", ":"))))
            elif kind == "leaderboard":
                for p in (m.get("traders") or []) + (m.get("brokers") or []):
                    put(kind, dict(base, team_id = p.get("team_id"), role = p.get("role"),
                                   cash = _f(p.get("cash")), net_worth = _f(p.get("net_worth")),
                                   realized_pnl = _f(p.get("realized_pnl")),
                                   unrealized_pnl = _f(p.get("unrealized_pnl")),
                                   fees_paid = _f(p.get("total_fees_paid")),
                                   rebates = _f(p.get("total_rebates_earned")),
                                   trade_count = p.get("trade_count")))
    cols["book_snapshot"].update(depth)
    return cols, {"week": week, "t0": t0, "t0_from": how, "lines": dict(seen)}
