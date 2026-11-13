"""Week 7 readiness: flashes, the team message quota, cancel-sparing quotes,
and an O(1) hot path.

Trader timers run on a frozen TickClock, so no test depends on wall-clock time.
"""

from __future__ import annotations

import asyncio
import pathlib
import random
import sys
import time

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT), str(ROOT / "engine")]

from shared.messages import BookSnapshot, Signal

import team.broker as tb
from team.common import HotPathMeter, PriceTrack, RollingStats, TickClock
from team.trader import MyTrader, TakerBot

WEEK7 = {"scenario": {"flags": {"shorts_allowed": True}, "position_limit": 1000,
                      "order_quota": 8, "maintenance_fraction": 0.4,
                      "config": {"SSR_TRIGGER_PCT": 0.08, "SHORT_LOCATE_CAP": 1200,
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


def seat(**positions) -> MyTrader:
    clock = TickClock(now=lambda: 0.0)
    clock.note(100)
    t = MyTrader(clock=clock, roles={"signal", "probe", "ipo"}, team_id="stt_11_trader_1")
    t.portfolio = FakePortfolio(**positions)
    t.on_open(WEEK7)
    t._probe_queue.clear()
    t.on_event("FEE_SCHEDULE", "fees", {"taker": 0.004, "rebate": 0.0})
    for _ in range(5):
        t.on_book("AMD", 100.0)
    return t


def flash(t: MyTrader, pct: float = 0.02, sym: str = "AMD") -> None:
    t.on_event("FLASH", "flash", {"symbol": sym, "pct": pct, "revert_ticks": 2, "tick": 100})


# ── Rolling statistics ──────────────────────────────────────────────────
def test_rolling_stats_match_a_full_rescan():
    rng = random.Random(1)
    rs, xs = RollingStats(30), []
    for _ in range(500):
        x = rng.gauss(0, 1e-4)
        rs.push(x)
        xs.append(x)
    w = xs[-30:]
    m = sum(w) / 30
    assert rs.std() == pytest.approx((sum((v - m) ** 2 for v in w) / 30) ** 0.5, rel=1e-6)


def test_price_track_keeps_jumps_out_of_the_vol_and_flags_them():
    clock = iter(i * 0.2 for i in range(1000))
    track = PriceTrack(maxlen=100, window=30, jump_pct=0.01, now=lambda: next(clock))
    for i in range(40):
        track.append(100 + 0.01 * (i % 2))
    calm = track.returns.std()
    track.append(102.5)                                 # a flash
    assert track.last_jump > 0 and track.returns.std() == pytest.approx(calm)
    assert track.interval == pytest.approx(0.2)
    assert track.vol(0.5) == pytest.approx(calm * (0.5 / 0.2) ** 0.5)


def test_hot_path_meter_percentiles():
    m = HotPathMeter()
    for _ in range(99):
        m.record(30e-6)
    m.record(3e-3)
    assert m.pct(0.5) == 50 and m.pct(0.99) == 50 and m.max_us == pytest.approx(3000)


# ── Flashes: the trader ─────────────────────────────────────────────────
def test_fair_value_ignores_the_flashed_mark():
    t = seat()
    t.on_book("AMD", 102.0)
    assert t._fair["AMD"] == pytest.approx(100.0)
    for _ in range(25):                                 # a move that sticks is learned
        t.on_book("AMD", 102.0)
    assert t._fair["AMD"] == pytest.approx(102.0)


def test_up_flash_is_faded_at_once_with_a_fee_aware_limit():
    t = seat()
    t._short_interest = {}
    flash(t, 0.02)
    order = t.on_tick(FakeMarket(AMD=(101.9, 102.1)), t.portfolio)
    assert order.side == "sell" and order.symbol == "AMD"
    edge = 2 * 0.004 + 15 / 1e4
    assert order.price == pytest.approx(100.0 * (1 + edge), abs=0.011)
    assert order.quantity == 250                        # $25k at $100
    t.note_sent(order)
    assert t.flash_log[0]["acted"] and t.flash_log[0]["react_ms"] is not None


def test_a_filled_fade_buys_back_once_the_flash_reverts():
    t = seat()
    flash(t, 0.02)
    order = t.on_tick(FakeMarket(AMD=(101.9, 102.1)), t.portfolio)
    t.note_sent(order)
    t.on_fill("sell", "AMD", order.quantity, 101.8)
    t.portfolio.positions["AMD"] = -order.quantity
    t.clock.note(100.5)
    assert t.on_tick(FakeMarket(AMD=(101.5, 101.7)), t.portfolio) is None   # not back yet
    t.clock.note(101.0)
    back = t.on_tick(FakeMarket(AMD=(100.0, 100.2)), t.portfolio)
    assert (back.side, back.quantity) == ("buy", order.quantity)
    t.on_fill("buy", "AMD", order.quantity, 100.2)
    t.portfolio.positions["AMD"] = 0
    t.clock.note(101.5)
    t.on_tick(FakeMarket(AMD=(100.0, 100.2)), t.portfolio)
    assert "AMD" not in t._flash
    row = t.flash_log[0]
    assert row["filled"] == order.quantity and row["pnl"] > 0


def test_time_stop_exits_a_flash_that_did_not_revert():
    t = seat()
    flash(t, -0.02)
    order = t.on_tick(FakeMarket(AMD=(97.9, 98.1)), t.portfolio)
    assert order.side == "buy"
    t.on_fill("buy", "AMD", 100, 98.1)
    t.clock.note(103.5)                                 # past revert (2) + grace (1)
    out = t.on_tick(FakeMarket(AMD=(97.5, 97.7)), t.portfolio)
    assert (out.side, out.quantity) == ("sell", 100)


def test_unfilled_fade_is_dropped_after_the_ioc_dies():
    t = seat()
    flash(t, 0.015)
    order = t.on_tick(FakeMarket(AMD=(100.9, 101.1)), t.portfolio)
    t.note_sent(order)
    t.clock.note(101.2)                                 # IOC dead, too late to retry
    assert t.on_tick(FakeMarket(AMD=(100.4, 100.6)), t.portfolio) is None
    assert "AMD" not in t._flash and t.flash_log[0]["acted"]


def test_scarce_borrow_shrinks_the_short_and_a_long_can_still_be_sold():
    t = seat()
    t._short_interest = {"AMD": 1100}                   # locate cap 1200, headroom 50
    flash(t, 0.02)
    assert t.on_tick(FakeMarket(AMD=(101.9, 102.1)), t.portfolio).quantity == 50
    t2 = seat(AMD=80)
    t2._short_interest = {"AMD": 1200}
    flash(t2, 0.02)
    assert t2.on_tick(FakeMarket(AMD=(101.9, 102.1)), t2.portfolio).quantity == 80


def test_no_flash_trade_in_a_halted_symbol_or_without_a_price():
    t = seat()
    t.on_event("TRADING_HALT", "halted", {"symbol": "AMD", "duration": 300})
    flash(t, 0.02)
    flash(t, 0.02, sym="ZZZ")
    assert not t._flash and [r["why"] for r in t.flash_log] == ["halted", "no fair price yet"]


def test_percent_given_as_a_whole_number_is_read_as_percent():
    t = seat()
    flash(t, 2.0)
    assert t._flash["AMD"].pct == pytest.approx(0.02)


# ── The trader's share of the team quota ────────────────────────────────
def _bot(t: MyTrader) -> tuple[TakerBot, list]:
    bot = TakerBot(t)
    bot.portfolio = t.portfolio
    t.portfolio = bot.portfolio
    sent: list = []

    async def fake_send(msg):
        sent.append(msg)
    bot._send = fake_send
    bot.market = bot.market                             # attach the wrappers
    return bot, sent


def test_trader_holds_itself_to_two_orders_a_second_but_a_flash_may_burst():
    t = seat()
    bot, sent = _bot(t)
    bot.portfolio.cash = 100_000.0
    for _ in range(3):
        asyncio.run(bot._place_order(Signal(symbol="MSFT", side="buy", quantity=1, price=400.0)))
    assert len(sent) == 2
    flash(t, -0.02)                                     # a live flash marks AMD urgent
    for _ in range(3):
        asyncio.run(bot._place_order(Signal(symbol="AMD", side="buy", quantity=1, price=100.0)))
    assert len(sent) == 4


def test_a_rate_limit_reject_pauses_ordinary_orders():
    t = seat()
    bot, sent = _bot(t)
    t.on_reject("RATE_LIMITED", "Message quota exceeded — you may send 8 order/cancel messages")
    asyncio.run(bot._place_order(Signal(symbol="MSFT", side="buy", quantity=1, price=400.0)))
    assert not sent and t.rate_limited == 1
    t.clock.note(101.5)
    asyncio.run(bot._place_order(Signal(symbol="MSFT", side="buy", quantity=1, price=400.0)))
    assert len(sent) == 1


def test_trade_tape_is_a_bounded_deque():
    bot, _ = _bot(seat())
    for _ in range(600):
        bot.market.update_trade(object())
    assert len(bot.market._recent_trades) == 500


# ── The broker ──────────────────────────────────────────────────────────
def _broker() -> tb.BudgetedBrokerBot:
    return tb.BudgetedBrokerBot(tb.MyBroker())


def test_broker_quotes_one_level_and_fits_the_week7_quota():
    assert tb._broker_config.QUOTE_LEVELS == 1
    b = _broker()
    b._budget = min(tb.MESSAGE_BUDGET, max(2, 8 - tb.TRADER_RESERVE))
    assert b._budget + 2 < 8                            # broker + trader steady state
    b.state.resting_orders["AAPL"] = {"buy_id": "b", "sell_id": "s",
                                      "buy_ids": ["b"], "sell_ids": ["s"]}
    assert b._requote_cost("AAPL") == 4


def test_broker_requotes_only_past_half_its_half_width():
    b = _broker()
    b._owner._half_spread["AAPL"] = 0.30
    b.state.last_quote_prices["AAPL"] = 200.0
    b.state.last_quote_time["AAPL"] = time.time()
    b.state.resting_orders["AAPL"] = {"buy_qty": 20, "sell_qty": 20}
    assert not b.needs_requote("AAPL", 200.14)
    assert b.needs_requote("AAPL", 200.16)
    b.state.resting_orders["AAPL"]["sell_qty"] = 0      # a side filled
    assert b.needs_requote("AAPL", 200.0)


def test_broker_freezes_a_flashed_symbol():
    b = _broker()
    asyncio.run(b._on_session_event(type("E", (), {"event": "FLASH", "message": "",
                                                   "data": {"symbol": "AAPL", "pct": 0.02,
                                                            "revert_ticks": 2}})()))
    assert b._is_frozen("AAPL") and not b._is_frozen("MSFT")


def test_broker_spots_a_jump_before_the_event_and_backs_off_after_a_rate_limit():
    b = _broker()
    for px in (200.0, 200.01, 204.0):
        b.state.price_history.setdefault("NVDA").append(px)
    assert b._is_frozen("NVDA")
    b._budget = 5
    assert b._room() == 5
    b._on_reject("RATE_LIMITED", "Message quota exceeded — you may send 8 order/cancel messages")
    assert b._room() == 0 and b.rate_limited == 1


def test_broker_width_uses_the_rolling_vol():
    b = _broker()
    track = b.state.price_history.setdefault("AMD")
    rng = random.Random(3)
    px = 100.0
    for _ in range(40):
        px *= 1 + rng.gauss(0, 0.003)
        track.append(px)
    width = b._owner.spread("AMD", px, track)
    assert width > px * tb.MIN_SPREAD_BPS / 1e4          # vol, not the floor


def test_new_listing_history_is_a_price_track_too():
    b = _broker()
    snap = BookSnapshot(symbol="VLTA", bids=[], asks=[], mid_price=30.0, spread=0.0, ref_price=30.0)
    b.state.price_history.setdefault(snap.symbol, None).append(snap.ref_price)
    assert isinstance(b.state.price_history["VLTA"], PriceTrack)


def test_a_quote_cancelled_by_self_trade_prevention_gets_requoted():
    b = _broker()
    b.state.last_quote_prices["MSFT"] = 415.0
    b.state.last_quote_time["MSFT"] = time.time()
    b.state.resting_orders["MSFT"] = {"buy_qty": 20, "sell_qty": 20}
    b._owner._half_spread["MSFT"] = 0.6
    assert not b.needs_requote("MSFT", 415.0)
    b._on_reject("STP_CANCEL", "Your resting sell 20 MSFT @ 416.98 was cancelled by "
                               "self-trade prevention (your own order crossed it)")
    assert b.needs_requote("MSFT", 415.0)


def test_the_venues_pre_flash_price_is_the_fair_value_when_sent():
    t = seat()
    t.on_event("FLASH", "flash", {"symbol": "AMD", "pct": 0.02, "revert_ticks": 2,
                                  "tick": 100, "start_price": 99.5})
    assert t._flash["AMD"].fair == 99.5


def test_a_probe_that_never_fills_gives_up():
    t = seat()
    t._probe_queue = ["MSFT"]
    m = FakeMarket(MSFT=(414.9, 415.1))
    sent = 0
    for i in range(20):
        t.clock.note(104 + i * 2)                       # past the delay and each IOC's life
        if t.on_tick(m, t.portfolio) is not None:
            sent += 1
    assert sent == 3 and t._probe.phase == "done"


def test_week_switch_resets_the_counters_and_the_broker_budget_fits_week7_from_the_start():
    b = _broker()
    assert b._budget + 2 < 8                            # before any rules have arrived
    b.msgs = b.cancels = 50
    asyncio.run(b._on_session_event(type("E", (), {"event": "WEEK_CHANGED", "message": "",
                                                   "data": {"week": 7, "order_quota": 8}})()))
    assert b.msgs == 0 and b._budget == 5
    t = seat()
    flash(t, 0.02)
    t.msgs_sent = 9
    t.on_event("WEEK_CHANGED", "Week 7", {"week": 7, "order_quota": 8})
    assert t.msgs_sent == 0 and t.flash_log == []
