"""The replay and live-analysis tools, on a tiny hand-written recording."""

from __future__ import annotations

import gzip
import json
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from Phase2.event_gap import GapParams  # noqa: E402
from Phase2.live_compare import analyse, realized_by_print, trading_curve  # noqa: E402
from Phase2.tape_replay import load_tape, replay  # noqa: E402

FEE = 0.004


def book(tick: int, bid: float, ask: float, size: int = 50, ref: float = 100.0) -> dict:
    return {"tick": tick, "msg": {"type": "book_snapshot", "symbol": "X", "bids": [[bid, size]],
                                  "asks": [[ask, size]], "mid_price": (bid + ask) / 2,
                                  "spread": ask - bid, "ref_price": ref}}


def event(tick: int, name: str, **data: object) -> dict:
    return {"tick": tick, "msg": {"type": "session_event", "event": name, "message": name,
                                  "data": data}}


def write(path: pathlib.Path, records: list[dict]) -> str:
    with gzip.open(path, "wt") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    return str(path)


def good_news(tmp_path: pathlib.Path, halt: bool = False) -> str:
    """X at 100 prints +5% (fair 105). The ask stays stale at 103 for a tick,
    then the book reprices to 105.5 / 106."""
    recs = [event(1, "SESSION_OPEN", week=6,
                  scenario={"flags": {"shorts_allowed": True}, "position_limit": 1000}),
            book(1, 99.9, 100.1), book(2, 99.9, 100.1),
            event(3, "CALENDAR_EVENT", kind="earnings", pct=0.05, affected=["X"]),
            book(3, 99.9, 103.0), book(4, 99.9, 103.0, ref=104.0)]
    if halt:
        recs.insert(3, event(3, "TRADING_HALT", symbol="X", duration=300))
        recs.append(event(9, "TRADING_RESUMED", symbol="X"))
    recs += [book(t, 105.5, 106.0, ref=105.6) for t in range(5, 60)]
    return write(tmp_path / "tape.jsonl.gz", recs)


def test_replay_takes_the_stale_ask_and_exits_at_fair_value(tmp_path):
    r = replay(load_tape(good_news(tmp_path)), GapParams(), latency=0, fee=FEE)
    plan = r["plans"][0]
    assert (plan["bought"], plan["sold"]) == (50, 50)
    assert plan["pnl"] == pytest.approx(50 * (105.5 - 103.0) - 50 * (103.0 + 105.5) * FEE)


def test_a_later_reaction_misses_the_stale_quote(tmp_path):
    r = replay(load_tape(good_news(tmp_path)), GapParams(), latency=2, fee=FEE)
    assert r["traded"] == 0


def test_two_seats_share_one_book(tmp_path):
    r = replay(load_tape(good_news(tmp_path)), GapParams(), latency=0, fee=FEE, seats=2)
    assert r["pnl_by_seat"][0] > 0 and r["pnl_by_seat"][1] == 0   # nothing left for seat 2
    one = replay(load_tape(good_news(tmp_path)), GapParams(), latency=0, fee=FEE)
    assert r["pnl"] == pytest.approx(one["pnl"])


def test_nothing_trades_in_a_halted_symbol(tmp_path):
    r = replay(load_tape(good_news(tmp_path, halt=True)), GapParams(), latency=0, fee=FEE)
    assert r["traded"] == 0


def our_session(tmp_path: pathlib.Path) -> str:
    """trader_1 buys 10 X after a print and sells them 5 ticks later; trader_2
    is allocated 4 VLTA shares in an IPO at $34 and sells them at $33."""
    def fill(tick: int, sym: str, qty: int, px: float, buyer: str, seller: str) -> dict:
        taker = buyer if buyer.startswith("stt_11_trader") else seller
        return {"tick": tick, "msg": {"type": "trade_execution", "trade_id": f"{tick}{sym}",
                                      "symbol": sym, "price": px, "quantity": qty,
                                      "buyer_id": buyer, "seller_id": seller, "aggressor": "buy",
                                      "fee": qty * px * FEE, "taker_id": taker}}
    recs = [event(1, "SESSION_OPEN", week=6, scenario={"position_limit": 1000}),
            book(1, 99.9, 100.1),
            event(2, "CALENDAR_EVENT", kind="earnings", pct=0.05, affected=["X"]),
            fill(3, "X", 10, 103.0, "stt_11_trader_1", "house"),
            event(4, "IPO_PRICED", symbol="VLTA"),
            event(6, "IPO_LISTED", symbol="VLTA"),
            book(7, 104.9, 105.1, ref=105.0),
            fill(8, "X", 10, 105.0, "house", "stt_11_trader_1"),
            fill(9, "VLTA", 4, 33.0, "house", "stt_11_trader_2"),
            event(10, "TRADING_HALT", symbol="X", duration=300),
            event(20, "TRADING_RESUMED", symbol="X"),
            fill(22, "X", 1, 105.0, "stt_11_trader_1", "house")]
    recs[4]["msg"]["message"] = "VLTA priced at $34.00 — 4 shares subscribed for 4 offered"
    recs[5]["msg"]["message"] = "VLTA is now trading — offered at $34.00."
    return write(tmp_path / "ours.jsonl.gz", recs)


def test_analysis_reports_activity_after_a_resume_and_no_liquidation(tmp_path):
    a = analyse(our_session(tmp_path))
    assert a["liquidations"] == []
    assert a["resumes"][0]["trader_first_fill"] == 2
    assert a["peak_from_fills"]["X"] == 10


def test_print_pnl_is_net_of_fees_and_leaves_out_unrelated_fills(tmp_path):
    a = analyse(our_session(tmp_path))
    row = next(r for r in realized_by_print(a) if r["symbol"] == "X")
    assert row["open_shares"] == 1                  # the post-resume buy is still X's
    fees = (10 * 103.0 + 10 * 105.0 + 1 * 105.0) * FEE   # we were the taker on all three
    assert row["pnl"] == pytest.approx(10 * 2.0 - fees)


def test_trading_curve_books_the_ipo_allocation_at_the_offer(tmp_path):
    path = our_session(tmp_path)
    curve = dict(trading_curve(path, analyse(path)))
    fees = (10 * 103.0 + 10 * 105.0 + 1 * 105.0 + 4 * 33.0) * FEE
    assert curve[max(curve)] == pytest.approx(10 * 2.0 + 4 * (33.0 - 34.0) - fees)
