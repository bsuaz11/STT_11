"""Week 6 readiness: the signal, the hard limits, halts and the IPO bid.

Everything runs on a frozen TickClock, so no test depends on wall-clock time.
"""

from __future__ import annotations

import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT), str(ROOT / "engine")]

from shared.messages import Signal  # noqa: E402

from Phase2.event_gap import EventGapSignal, GapParams  # noqa: E402
from team.common import HaltTracker, TickClock  # noqa: E402
from team.trader import MyTrader  # noqa: E402

SCENARIO = {"scenario": {"flags": {"shorts_allowed": True}, "position_limit": 1000,
                         "maintenance_fraction": 0.4,
                         "config": {"SSR_TRIGGER_PCT": 0.06, "SHORT_LOCATE_CAP": 2000,
                                    "LULD_BAND_PCT": 0.08}}, "tick": 100}


class FakeMarket:
    def __init__(self, **touch: tuple) -> None:
        self.touch = touch

    def symbols(self):
        return list(self.touch)

    def best_bid(self, s):
        return self.touch.get(s, (None, None))[0]

    def best_ask(self, s):
        return self.touch.get(s, (None, None))[1]

    def mid_price(self, s):
        bid, ask = self.touch.get(s, (None, None))
        return (bid + ask) / 2 if bid and ask else None


class FakePortfolio:
    def __init__(self, cash: float = 100_000.0, **positions: int) -> None:
        self.cash = cash
        self.positions = dict(positions)
        self.realized_pnl = 0.0
        self.total_fees_paid = 0.0

    def can_buy(self, symbol, qty, price):
        return self.cash >= qty * price


def seat(roles=("signal", "probe", "ipo"), portfolio=None, open_session=True) -> MyTrader:
    clock = TickClock(now=lambda: 0.0)
    clock.note(100)
    t = MyTrader(clock=clock, roles=set(roles), team_id="stt_11_trader_1")
    t.portfolio = portfolio or FakePortfolio()
    if open_session:
        t.on_open(SCENARIO)
    return t


# ── The signal ──────────────────────────────────────────────────────────
def gap(pct=0.05, **kw) -> EventGapSignal:
    s = EventGapSignal(params=GapParams(buffer_bps=20, **kw), taker_fee=0.004)
    s.on_print("X", pct, 100.0, tick=10, position=0)
    return s


def test_edge_is_two_fees_plus_buffer():
    assert gap().edge == pytest.approx(0.01)


def test_buys_only_below_the_gate_and_limit_is_the_gate():
    s = gap()                                       # fair 105, gate 103.95
    assert s.decide("X", 11, 103.0, 104.5, 0, 100, 100) is None
    order = s.decide("X", 11, 103.0, 103.5, 0, 100, 100)
    assert (order.side, order.quantity, order.price, order.reason) == ("buy", 100, 103.95, "entry")


def test_bad_news_sells_only_above_the_gate():
    s = gap(pct=-0.05)                              # fair 95, gate 95.95
    assert s.decide("X", 11, 95.5, 97.0, 0, 100, 100) is None
    order = s.decide("X", 11, 96.5, 97.0, 0, 100, 100)
    assert (order.side, order.price) == ("sell", 95.95)


def test_exits_at_target_then_closes_with_the_pnl_net_of_fees():
    s = gap()
    s.on_fill("X", "buy", 100, 103.0)
    order = s.decide("X", 12, 105.0, 105.5, 100, 0, 100)
    assert (order.side, order.quantity, order.reason) == ("sell", 100, "target")
    s.on_fill("X", "sell", 100, 105.0)
    assert s.decide("X", 25, 105.0, 105.5, 0, 0, 100) is None
    result = s.closed[0]
    assert result.pnl == pytest.approx(200 - (10300 + 10500) * 0.004)


def test_time_stop_exits_even_at_a_loss():
    s = gap(max_hold_ticks=30)
    s.on_fill("X", "buy", 50, 103.0)
    order = s.decide("X", 40, 100.0, 100.5, 50, 0, 50)
    assert (order.side, order.quantity, order.reason) == ("sell", 50, "time_stop")


def test_stops_opening_after_the_entry_window():
    s = gap(max_entry_ticks=5)
    assert s.decide("X", 16, 100.0, 100.5, 0, 100, 100) is None
    assert "X" not in s.plans


def test_empty_book_side_never_crashes_and_waits():
    s = gap()
    assert s.decide("X", 11, None, None, 0, 100, 100) is None
    s.on_fill("X", "buy", 10, 103.0)
    assert s.decide("X", 40, None, 110.0, 10, 0, 10) is None
    assert "X" in s.plans                           # still has to exit


def test_exits_return_to_the_position_held_before_the_print():
    s = EventGapSignal(params=GapParams(buffer_bps=20), taker_fee=0.004)
    s.on_print("X", 0.05, 100.0, tick=10, position=20)
    order = s.decide("X", 40, 100.0, 100.5, 30, 0, 30)
    assert order.quantity == 10                     # the 20 granted shares stay


def test_fee_change_moves_the_gate():
    s = gap()
    s.set_fee(0.003)
    assert s.edge == pytest.approx(0.008)


# ── Hard limits ─────────────────────────────────────────────────────────
def buy(sym="AMD", qty=100, px=165.0):
    return Signal(symbol=sym, side="buy", quantity=qty, price=px)


def sell(sym="AMD", qty=100, px=165.0):
    return Signal(symbol=sym, side="sell", quantity=qty, price=px)


def test_scenario_limit_is_read_and_seats_plus_broker_fit_inside_it():
    t = seat()
    long_cap, short_cap = t._seat_caps("AMD")
    assert (long_cap, short_cap) == (415, 415)
    assert 2 * long_cap + 150 <= 1000 - 20


def test_gate_shrinks_to_the_seat_cap_and_counts_orders_in_flight():
    t, m = seat(), FakeMarket(AMD=(164.9, 165.1))
    t.on_book("AMD", 165.0)
    p = FakePortfolio(AMD=405)
    first = t.gate(buy(), m, p)
    assert first.quantity == 10
    t.note_sent(first)
    assert t.gate(buy(qty=5), m, p) is None


def test_gate_respects_the_team_limit_across_seats():
    t, m = seat(), FakeMarket(AMD=(164.9, 165.1))
    t.on_book("AMD", 165.0)
    t._team_others["AMD"] = 850
    assert t.gate(buy(qty=200), m, FakePortfolio(AMD=0)).quantity == 130


def test_selling_down_is_never_blocked_by_a_cap():
    t, m = seat(), FakeMarket(AMD=(164.9, 165.1))
    t.on_book("AMD", 165.0)
    t._team_others["AMD"] = 900
    assert t.gate(sell(qty=400), m, FakePortfolio(AMD=415)).quantity == 400


def test_gross_leverage_caps_a_large_short():
    t, m = seat(), FakeMarket(NFLX=(719.0, 721.0))
    t.on_book("NFLX", 720.0)
    order = t.gate(sell("NFLX", 400, 719.0), m, FakePortfolio())
    assert order.quantity * 720 <= 150_000


def test_limit_price_is_clamped_inside_the_luld_band():
    t, m = seat(), FakeMarket(AMD=(164.9, 165.1))
    t.on_book("AMD", 100.0)
    assert t.gate(buy(qty=1, px=120.0), m, FakePortfolio()).price <= 108.0
    assert t.gate(sell(qty=1, px=80.0), m, FakePortfolio(AMD=1)).price >= 92.0


def test_near_liquidation_it_only_reduces_and_unwinds_the_biggest_position():
    t, m = seat(), FakeMarket(NFLX=(899.0, 901.0), AMD=(164.9, 165.1))
    t.on_book("NFLX", 900.0)
    t.on_book("AMD", 165.0)
    t._probe_queue.clear()
    p = FakePortfolio(cash=200_000.0, NFLX=-200)    # equity 20k, line at 60k
    order = t.on_tick(m, p)
    assert t._risk_off and (order.symbol, order.side, order.quantity) == ("NFLX", "buy", 200)
    assert t.gate(buy(qty=5), m, p) is None


def test_orders_are_refused_during_an_auction():
    t, m = seat(), FakeMarket(AMD=(164.9, 165.1))
    t.on_event("SESSION_PRECLOSE", "closing", {"ticks": 10})
    assert t.on_tick(m, FakePortfolio()) is None
    assert t.gate(buy(), m, FakePortfolio()) is None


# ── Halts ───────────────────────────────────────────────────────────────
def test_halted_symbol_gets_no_orders_then_activity_resumes():
    t = seat(portfolio=FakePortfolio(cash=100_000.0))
    m = FakeMarket(MSFT=(414.9, 415.1), AMD=(164.9, 165.1))
    t.on_book("AMD", 165.0)
    t._probe_queue.clear()
    t.on_event("TRADING_HALT", "halted", {"symbol": "AMD", "duration": 300})
    assert t.gate(buy(), m, t.portfolio) is None
    t.clock.note(350)
    t.on_event("TRADING_RESUMED", "resumed", {"symbol": "AMD"})
    assert t.gate(buy(qty=5), m, t.portfolio) is not None
    assert "AMD" in t._probe_queue
    t.clock.note(354)                               # past the refill delay
    t.on_tick(m, t.portfolio)
    t.clock.note(358)
    order = t.on_tick(m, t.portfolio)
    assert (order.symbol, order.side) == ("AMD", "buy")


def test_a_missed_resume_expires_and_a_reject_reveals_a_halt():
    clock = TickClock(now=lambda: 0.0)
    clock.note(100)
    halts = HaltTracker(clock)
    halts.on_reject("AMD trading is halted. Resume in 30s")
    assert halts.is_halted("AMD")
    clock.note(136)
    assert not halts.is_halted("AMD")
    assert halts.expired() == ["AMD"]


def test_market_halt_blocks_every_symbol_and_market_resume_clears_it():
    clock = TickClock(now=lambda: 0.0)
    clock.note(100)
    halts = HaltTracker(clock)
    halts.on_event("TRADING_HALT", {"symbol": "AMD", "duration": 300})
    halts.on_event("MARKET_HALT", {"duration": 900})
    assert halts.is_halted("MSFT")
    assert halts.on_event("MARKET_RESUMED", {}) == ["AMD"]
    assert not halts.is_halted("MSFT") and not halts.is_halted("AMD")


# ── A print through the seat ────────────────────────────────────────────
def test_print_uses_the_mark_from_before_it_and_holds_through_a_halt():
    t = seat()
    m = FakeMarket(NFLX=(700.0, 700.5))
    t.on_book("NFLX", 700.0)
    t._probe_queue.clear()
    t.on_event("CALENDAR_EVENT", "earnings", {"kind": "earnings", "pct": 0.10,
                                              "affected": ["NFLX"], "tick": 100})
    assert t.signal.plans["NFLX"].pre_mark == 700.0
    order = t.on_tick(m, t.portfolio)
    assert order.side == "buy" and order.price <= 770 * (1 - t.signal.edge)
    t.portfolio.positions["NFLX"] = order.quantity
    t.on_fill("buy", "NFLX", order.quantity, 700.5)
    t.on_event("TRADING_HALT", "halted", {"symbol": "NFLX", "duration": 300})
    t.clock.note(200)
    assert t.on_tick(FakeMarket(NFLX=(770.0, 771.0)), t.portfolio) is None
    t.clock.note(402)
    t.on_event("TRADING_RESUMED", "resumed", {"symbol": "NFLX"})
    exit_order = t.on_tick(FakeMarket(NFLX=(770.0, 771.0)), t.portfolio)
    assert (exit_order.side, exit_order.quantity) == ("sell", order.quantity)


def test_venue_fee_schedule_reaches_the_signal():
    t = seat()
    t.on_event("FEE_SCHEDULE", "fees", {"taker": 0.003, "rebate": 0.0005})
    assert t.signal.taker_fee == 0.003


def test_shorts_stop_under_the_short_sale_rule():
    t = seat()
    t._open_mid["AMD"] = 165.0
    p = FakePortfolio()
    assert t._short_room("AMD", p, 164.0) == 415
    assert t._short_room("AMD", p, 150.0) == 0      # within the margin of the trigger
    t.on_event("SSR_ON", "ssr", {"symbol": "AMD"})
    assert t._short_room("AMD", FakePortfolio(AMD=20), 164.0) == 20


# ── IPO ─────────────────────────────────────────────────────────────────
def test_ipo_bid_is_top_of_range_and_both_seats_stay_under_the_limit():
    t = seat()
    t._cash = 100_000.0
    qty, px = t.on_ipo("VLTA", 30.0, 34.0, 3500, {})
    assert px == 34.0 and qty * px <= 15_000 and qty > 0
    assert 2 * qty + 50 <= 1000 - 20                # two seats + the broker's listing cap


def test_ipo_bid_retries_until_confirmed_and_shrinks_on_reject():
    t = seat()
    t._cash = 100_000.0
    qty, _ = t.on_ipo("VLTA", 30.0, 34.0, 3500, {})
    assert t.pending_ipo() is None                  # just sent
    t.clock.note(106)
    assert t.pending_ipo().quantity == qty
    t.on_reject("IPO_REJECTED", "needs more cash")
    assert t.pending_ipo().quantity == qty // 2
    t.on_event("IPO_SUB_RESULT", "ok", {"ok": True, "symbol": "VLTA"})
    t.clock.note(200)
    assert t.pending_ipo() is None


def test_seat_without_the_ipo_role_passes():
    assert seat(roles=("signal",)).on_ipo("VLTA", 30.0, 34.0, 3500, {}) is None


def test_listing_exit_stops_out_below_the_offer_and_holds_above_it():
    t = seat()
    t._probe_queue.clear()
    t.on_event("IPO_LISTED", "listed", {"symbol": "VLTA", "offer_price": 34.0})
    p = FakePortfolio(VLTA=300)
    t.on_book("VLTA", 34.5)
    assert t.on_tick(FakeMarket(VLTA=(34.4, 34.6)), p) is None
    t.on_book("VLTA", 32.0)
    order = t.on_tick(FakeMarket(VLTA=(31.9, 32.1)), p)
    assert (order.symbol, order.side, order.quantity) == ("VLTA", "sell", 300)


# ── Restart safety ──────────────────────────────────────────────────────
def test_a_restarted_process_recovers_the_trade_it_was_in(tmp_path):
    path = tmp_path / "seat.json"
    t = MyTrader(clock=seat().clock, roles={"signal"}, team_id="stt_11_trader_1", state_path=path)
    t.portfolio = FakePortfolio()
    t.on_open(SCENARIO)
    t.on_book("INTC", 22.0)
    t.on_event("CALENDAR_EVENT", "earnings", {"pct": -0.05, "affected": ["INTC"], "tick": 100})
    t.on_event("IPO_LISTED", "listed", {"symbol": "VLTA", "offer_price": 30.0})

    again = MyTrader(clock=t.clock, roles={"signal"}, team_id="stt_11_trader_1", state_path=path)
    again.portfolio = FakePortfolio(INTC=-200)
    again.on_open(dict(SCENARIO, late_join=True))
    assert again.signal.plans["INTC"].base == 0 and "VLTA" in again._listings
    again.clock.note(140)                           # past the time stop
    order = again.on_tick(FakeMarket(INTC=(20.9, 21.0)), again.portfolio)
    assert (order.symbol, order.side, order.quantity) == ("INTC", "buy", 200)


def test_a_new_session_forgets_the_last_one(tmp_path):
    path = tmp_path / "seat.json"
    t = MyTrader(clock=seat().clock, roles={"signal"}, team_id="stt_11_trader_1", state_path=path)
    t.portfolio = FakePortfolio()
    t.on_open(SCENARIO)
    t.on_book("INTC", 22.0)
    t.on_event("CALENDAR_EVENT", "earnings", {"pct": -0.05, "affected": ["INTC"], "tick": 100})
    t.on_open(SCENARIO)
    assert not t.signal.plans


def test_a_buy_never_asks_for_more_cash_than_the_seat_has():
    t, m = seat(), FakeMarket(MSFT=(414.9, 415.1))
    t.on_book("MSFT", 415.0)
    order = t.gate(buy("MSFT", 400, 415.0), m, FakePortfolio(cash=41_500.0, MSFT=-400))
    assert order.quantity * 415.0 <= 41_500.0


def test_an_unfilled_remainder_stops_counting_once_the_ioc_has_filled():
    t, m = seat(), FakeMarket(AMD=(164.9, 165.1))
    t.on_book("AMD", 165.0)
    p = FakePortfolio(AMD=405)
    t.note_sent(t.gate(buy(), m, p))
    t.on_fill("buy", "AMD", 4, 165.0)
    assert t._in_flight("AMD", "buy") == 6          # the same instant: still in doubt
    t.clock.note(101)
    assert t._in_flight("AMD", "buy") == 0


def test_a_print_during_a_probe_keeps_signal_and_probe_shares_apart():
    t = seat()
    t.portfolio = FakePortfolio(MSFT=5)
    t.on_book("MSFT", 415.0)
    t._probe_queue.clear()
    from team.trader import RoundTrip
    t._probe = RoundTrip("MSFT")
    t._probe.on_fill("buy", 5, t.clock.now())
    t.on_event("CALENDAR_EVENT", "earnings", {"pct": -0.05, "affected": ["MSFT"], "tick": 100})
    t.on_fill("sell", "MSFT", 100, 410.0)
    plan = t.signal.plans["MSFT"]
    assert plan.sold == 100 and plan.base == 5
    assert t._probe.held == 5                       # the probe still owns exactly its own


def test_a_restart_keeps_the_fills_already_charged_to_a_plan(tmp_path):
    path = tmp_path / "seat.json"
    t = MyTrader(clock=seat().clock, roles={"signal"}, team_id="stt_11_trader_1", state_path=path)
    t.portfolio = FakePortfolio()
    t.on_open(SCENARIO)
    t.on_book("INTC", 22.0)
    t.on_event("CALENDAR_EVENT", "earnings", {"pct": -0.05, "affected": ["INTC"], "tick": 100})
    t.on_fill("sell", "INTC", 60, 21.5)
    again = MyTrader(clock=t.clock, roles={"signal"}, team_id="stt_11_trader_1", state_path=path)
    again.portfolio = FakePortfolio(INTC=-60)
    again.on_open(dict(SCENARIO, late_join=True))
    assert again.signal.plans["INTC"].sold == 60


# ── One process per seat ────────────────────────────────────────────────
class FakeBoard:
    def __init__(self, positions: dict, tick: int = 100) -> None:
        self.tick = tick
        self.traders = [{"team_id": "stt_11_trader_1", "net_worth": 100_000.0,
                         "positions": positions}]
        self.brokers = []


def test_a_process_that_never_hears_its_fills_stands_down():
    t = seat(portfolio=FakePortfolio(INTC=-20))     # the stale view
    for _ in range(2):
        t.on_leaderboard(FakeBoard({"INTC": 91}))
    assert not t._stood_down                        # two boards can straddle a fill
    t.on_leaderboard(FakeBoard({"INTC": 91}))
    assert t._stood_down
    assert t.on_tick(FakeMarket(INTC=(16.4, 16.5)), t.portfolio) is None
    assert t.gate(sell("INTC", 5, 16.4), FakeMarket(), t.portfolio) is None


def test_a_brief_disagreement_around_a_fill_is_not_a_duplicate():
    t = seat(portfolio=FakePortfolio(INTC=-20))
    t.on_leaderboard(FakeBoard({"INTC": 0}))
    t.on_leaderboard(FakeBoard({"INTC": -20}))
    t.on_leaderboard(FakeBoard({"INTC": 0}))
    assert not t._stood_down


def test_a_second_process_for_the_same_seat_will_not_start(tmp_path, monkeypatch):
    import team.common as common
    monkeypatch.setattr(common, "LOCK_DIR", tmp_path)
    first = common.single_instance("stt_11_trader_9")
    with pytest.raises(SystemExit):
        common.single_instance("stt_11_trader_9")
    first.close()
    common.single_instance("stt_11_trader_9").close()   # free again once the first exits
