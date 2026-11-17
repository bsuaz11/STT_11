"""Phase 3: one test (at least) per guard in team/risk.py, through the real
seats where the guard lives in a seat.

Each test is named after the guard it proves and checks both the trigger and
the resulting behaviour. RISK.md maps the guard table onto these names.
Everything runs on a frozen TickClock and a temporary kill directory.

Run:  PYTHONPATH=engine:. pytest tests/test_risk_guards.py -v
"""

from __future__ import annotations

import asyncio
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT), str(ROOT / "engine")]

from shared.messages import PlaceOrder, Signal

import team.broker as tb
from team.common import TickClock
from team.exchange import MyExchange
from team.risk import (
    BROKER_LIMITS,
    TRADER_LIMITS,
    KillSwitch,
    RiskManager,
    VenueGuard,
    VenueLimits,
    notional_room,
)
from team.trader import MyTrader, TakerBot

SCENARIO = {"scenario": {"flags": {"shorts_allowed": True}, "position_limit": 1000,
                         "order_quota": 8, "maintenance_fraction": 0.4,
                         "config": {"SSR_TRIGGER_PCT": 0.08, "SHORT_LOCATE_CAP": 2000,
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


class Clock:
    """A frozen tick clock the test moves by hand."""

    def __init__(self, t: float = 100.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def kill_dir(tmp_path, monkeypatch):
    import team.risk
    monkeypatch.setattr(team.risk, "KILL_DIR", tmp_path)
    return tmp_path


def seat(kill_dir: pathlib.Path, cash: float = 100_000.0, **positions: int) -> MyTrader:
    clock = TickClock(now=lambda: 0.0)
    clock.note(100)
    t = MyTrader(clock=clock, roles={"signal"}, team_id="stt_11_trader_1", kill_dir=kill_dir)
    t.portfolio = FakePortfolio(cash, **positions)
    t.on_open(SCENARIO)
    t.on_event("FEE_SCHEDULE", "fees", {"taker": 0.004, "rebate": 0.0})
    for sym, px in (("AMD", 100.0), ("MSFT", 400.0), ("NFLX", 700.0)):
        t.on_book(sym, px)
    return t


def advance(t: MyTrader, tick: float) -> None:
    """Move the clock, with the venue's book feed running (it never stops)."""
    t.clock.note(tick)
    for sym, px in list(t._ref.items()):
        t.on_book(sym, px)


def buy(sym="AMD", qty=100, px=100.0) -> Signal:
    return Signal(symbol=sym, side="buy", quantity=qty, price=px)


def sell(sym="AMD", qty=100, px=100.0) -> Signal:
    return Signal(symbol=sym, side="sell", quantity=qty, price=px)


def manager(kill_dir, limits=TRADER_LIMITS, capital=100_000.0) -> RiskManager:
    return RiskManager("stt_11_trader_1", limits, capital, kill_dir=kill_dir)


# ═══ 1. Hard limits, checked before the order is emitted ═══════════════
def test_limit_max_position_per_symbol_shrinks_the_order_before_it_is_sent(kill_dir):
    t = seat(kill_dir, AMD=400)
    cap = t._seat_caps("AMD")[0]                         # (1000 − 20 − 150) / 1 seat = 830
    order = t.gate(buy(qty=500), FakeMarket(AMD=(99.9, 100.1)), t.portfolio)
    assert order.quantity == cap - 400


def test_limit_gross_notional_caps_new_exposure(kill_dir):
    # $100k equity, 1.5× gross: already $140k gross → room for $10k = 100 AMD.
    pos = {"MSFT": 200, "NFLX": -85}                     # $80k + $59.5k = $139.5k gross
    room = notional_room("buy", 100.0, "AMD", pos, {"MSFT": 400, "NFLX": 700, "AMD": 100},
                         100_000.0, TRADER_LIMITS)
    assert room == 105
    t = seat(kill_dir, cash=100_000.0 - 80_000 + 59_500, **pos)
    order = t.gate(buy(qty=300), FakeMarket(AMD=(99.9, 100.1), MSFT=(399.9, 400.1),
                                            NFLX=(699.9, 700.1)), t.portfolio)
    assert order is not None and order.quantity <= 105


def test_limit_net_notional_caps_a_one_way_book(kill_dir):
    # Net cap 1.0× equity: $90k long already → only $10k more long, but shorts are free.
    pos, marks = {"MSFT": 225}, {"MSFT": 400.0, "AMD": 100.0}
    assert notional_room("buy", 100.0, "AMD", pos, marks, 100_000.0, TRADER_LIMITS) == 100
    assert notional_room("sell", 100.0, "AMD", pos, marks, 100_000.0, TRADER_LIMITS) > 100
    # The broker's net cap is tighter (0.5×): $45k of $100k → $5k room.
    assert notional_room("buy", 100.0, "AMD", {"MSFT": 112}, marks, 100_000.0,
                         BROKER_LIMITS) == 52


def test_limit_never_blocks_reducing_a_position(kill_dir):
    pos, marks = {"MSFT": 600}, {"MSFT": 400.0}        # 2.4× gross: far past both caps
    assert notional_room("buy", 400.0, "MSFT", pos, marks, 100_000.0, TRADER_LIMITS) == 0
    assert notional_room("sell", 400.0, "MSFT", pos, marks, 100_000.0, TRADER_LIMITS) >= 600


def test_limit_breaching_order_is_never_sent_on_the_wire(kill_dir):
    # Short $70k NFLX on $100k equity: net room for $30k more short = 300 AMD.
    t = seat(kill_dir, cash=170_000.0, NFLX=-100)
    t._short_interest = {}
    fake = t.portfolio
    bot = TakerBot(t)                                    # (swaps in its own portfolio)
    bot.portfolio = t.portfolio = fake
    sent: list = []

    async def fake_send(msg):
        sent.append(msg)
    bot._send = fake_send
    bot._market = FakeMarket(AMD=(99.9, 100.1), NFLX=(699.9, 700.1))
    asyncio.run(bot._place_order(sell(qty=500)))
    assert len(sent) == 1 and sent[0].quantity == 300   # cut to the net room
    t.portfolio.positions["AMD"] = -300
    t.portfolio.cash += 30_000
    t.clock.note(102)
    asyncio.run(bot._place_order(sell(qty=500)))
    assert len(sent) == 1                                # no room left: nothing sent


# ═══ 2. Stop-loss ═══════════════════════════════════════════════════════
def test_position_stop_flattens_the_loser_and_blocks_reentry(kill_dir):
    t = seat(kill_dir, cash=100_000.0 - 300 * 100.0, AMD=300)
    t.on_fill("buy", "AMD", 300, 100.0)
    advance(t, 101)
    m = FakeMarket(AMD=(97.9, 98.1))
    t.on_book("AMD", 98.0)                               # −2%: inside the 4% stop
    assert t.on_tick(m, t.portfolio) is None
    m = FakeMarket(AMD=(95.4, 95.6))
    t.on_book("AMD", 95.5)                               # −4.5%: past it
    order = t.on_tick(m, t.portfolio)
    assert (order.symbol, order.side, order.quantity) == ("AMD", "sell", 300)
    assert t.gate(order, m, t.portfolio) is not None     # the exit passes every limit
    t.portfolio.positions["AMD"] = 0
    advance(t, 110)
    assert t.gate(buy(px=95.6), m, t.portfolio) is None  # cool-down: no re-entry…
    advance(t, 102 + TRADER_LIMITS.stop_cooldown_ticks + 1)
    assert t.gate(buy(px=95.6), m, t.portfolio) is not None   # …until it ends


def test_position_stop_ignores_small_noise_below_the_dollar_floor(kill_dir):
    r = manager(kill_dir)
    r.on_fill("AMD", "buy", 10, 100.0)                   # $1,000 position
    d = r.update(100, 100_000.0, {"AMD": 10}, {"AMD": 90.0})   # −10% but only −$100
    assert d.flatten == []


def test_session_soft_stop_goes_reduce_only(kill_dir):
    t = seat(kill_dir, cash=100_000.0 - 400 * 100.0, AMD=400)
    m = FakeMarket(AMD=(99.9, 100.1), MSFT=(399.9, 400.1))
    t.on_tick(m, t.portfolio)                            # baseline: equity $100k
    t.risk.book.cooldown.clear()
    t.portfolio.cash -= 3_200                            # −$3.2k: past the $3k soft stop
    advance(t, 101)
    t.on_tick(m, t.portfolio)
    assert t.risk.session.soft and not t.risk.session.hard
    assert t.gate(buy("MSFT", 10, 400.0), m, t.portfolio) is None      # no new exposure
    assert t.gate(sell(qty=100), m, t.portfolio).quantity == 100       # reducing is fine


def test_session_hard_stop_trips_the_kill_switch_and_flattens(kill_dir):
    t = seat(kill_dir, cash=100_000.0 - 400 * 100.0, AMD=400)
    m = FakeMarket(AMD=(99.9, 100.1))
    t.on_tick(m, t.portfolio)                            # baseline
    t.portfolio.cash -= 5_500                            # −$5.5k: past the $5k hard stop
    advance(t, 101)
    order = t.on_tick(m, t.portfolio)
    assert t.risk.kill.reasons() == ["session_loss"]
    assert (order.side, order.quantity) == ("sell", 400)
    assert t.gate(order, m, t.portfolio) is not None     # the flatten goes out
    assert t.gate(buy("MSFT", 1, 400.0), m, t.portfolio) is None


# ═══ 3. Kill switch ═════════════════════════════════════════════════════
def test_kill_switch_manual_file_stops_orders_mid_session_and_releases(kill_dir):
    t = seat(kill_dir)
    m = FakeMarket(AMD=(99.9, 100.1))
    assert t.gate(buy(qty=10), m, t.portfolio) is not None
    (kill_dir / "KILL").write_text("stop\n")             # a human, mid-session
    advance(t, 101)
    assert t.on_tick(m, t.portfolio) is None
    assert t.risk.kill.reasons() == ["manual"]
    assert t.gate(buy(qty=10), m, t.portfolio) is None
    (kill_dir / "KILL").unlink()
    advance(t, 102)
    t.on_tick(m, t.portfolio)
    assert t.gate(buy(qty=10), m, t.portfolio) is not None


def test_kill_switch_manual_flatten_closes_positions_one_seat_only(kill_dir):
    t = seat(kill_dir, cash=60_000.0, AMD=400)
    (kill_dir / "KILL.stt_11_trader_2").write_text("flatten")   # another seat: not us
    advance(t, 101)
    m = FakeMarket(AMD=(99.9, 100.1))
    t.on_tick(m, t.portfolio)
    assert not t.risk.kill.active(101)
    (kill_dir / "KILL.stt_11_trader_1").write_text("flatten")
    advance(t, 102)
    order = t.on_tick(m, t.portfolio)
    assert (order.side, order.quantity) == ("sell", 400)
    assert t.gate(order, m, t.portfolio).quantity == 400


def test_kill_switch_script_writes_and_removes_the_file(kill_dir, capsys):
    import scripts.kill_switch as ks
    ks.KILL_DIR = kill_dir
    ks.main(["on", "--flatten", "--seat", "stt_11_broker_1"])
    assert (kill_dir / "KILL.stt_11_broker_1").read_text().strip() == "flatten"
    ks.main(["off", "--seat", "stt_11_broker_1"])
    assert not (kill_dir / "KILL.stt_11_broker_1").exists()


def test_kill_switch_auto_reject_storm_cools_off(kill_dir):
    t = seat(kill_dir)
    for _ in range(12):
        t.note_sent(buy())
    for _ in range(9):
        t.on_reject("INSUFFICIENT_CASH", "no")
    assert not t.risk.kill.active(t.clock.now())
    for _ in range(5):                                   # benign rejects don't count
        t.on_reject("ORDER_NOT_FOUND", "gone")
    assert not t.risk.kill.active(t.clock.now())
    t.on_reject("INSUFFICIENT_CASH", "no")               # 10 of 12: a storm
    assert t.risk.kill.reasons() == ["reject_storm"]
    assert t.gate(buy(qty=1), FakeMarket(AMD=(99.9, 100.1)), t.portfolio) is None
    advance(t, 100 + TRADER_LIMITS.reject_cooldown_ticks + 1)
    assert not t.risk.kill.active(t.clock.now())


def test_kill_switch_auto_reconnect_storm(kill_dir):
    r = manager(kill_dir)
    for tick in (0, 50, 100):
        r.on_connect(tick)
    assert not r.kill.active(100)                        # first connect + 2 reconnects
    r.on_connect(150)                                    # a 3rd reconnect within 300 ticks
    assert r.kill.reasons() == ["reconnects"]
    assert not r.kill.flatten(150)                       # stop sending; don't trade blind
    assert not r.kill.active(150 + TRADER_LIMITS.reconnect_cooldown_ticks)


def test_kill_switch_auto_stale_feed_and_recovery(kill_dir):
    r = manager(kill_dir)
    r.on_data(100)
    assert not r.update(110, None, {}, {}).kill
    assert r.update(116, None, {}, {}).kill              # 16 ticks of silence
    r.on_data(117)
    assert not r.update(117, None, {}, {}).kill


def test_new_session_resets_automatic_trips_but_not_a_manual_kill(kill_dir):
    r = manager(kill_dir)
    r.kill.trip("session_loss", 100, flatten=True)
    r.kill.trip("manual", 100)
    r.new_session(200)
    assert r.kill.reasons() == ["manual"]


# ═══ 4. Halts and shocks ════════════════════════════════════════════════
def test_halt_no_orders_while_halted_so_no_reject_spam(kill_dir):
    t = seat(kill_dir)
    t.on_event("TRADING_HALT", "halted", {"symbol": "AMD", "duration": 300})
    m = FakeMarket(AMD=(99.9, 100.1))
    for i in range(20):
        advance(t, 101 + i)
        assert t.gate(buy(qty=10), m, t.portfolio) is None
    assert t.risk.rejects.counts(t.clock.now()) == (0, 0)   # nothing sent, nothing rejected


def test_halt_reentry_waits_for_settle_and_a_two_sided_book(kill_dir):
    t = seat(kill_dir, cash=95_000.0, AMD=50)
    t.on_event("TRADING_HALT", "halted", {"symbol": "AMD", "duration": 300})
    advance(t, 400)
    t.on_event("TRADING_RESUMED", "resumed", {"symbol": "AMD"})
    m = FakeMarket(AMD=(99.9, 100.1))
    assert t.gate(buy(qty=10), m, t.portfolio) is None       # first tick: wait
    assert t.gate(sell(qty=50), m, t.portfolio) is not None  # an exit may go
    advance(t, 404)
    one_sided = FakeMarket(AMD=(None, 100.1))
    assert t.gate(buy(qty=10), one_sided, t.portfolio) is None
    wide = FakeMarket(AMD=(97.0, 103.0))                     # 600 bps: not settled
    assert t.gate(buy(qty=10), wide, t.portfolio) is None
    assert t.gate(buy(qty=10), m, t.portfolio) is not None   # settled: deliberate re-entry


def test_shock_broker_pulls_quotes_and_trader_reads_it_as_a_print(kill_dir):
    b = _broker(kill_dir)
    asyncio.run(b._on_session_event(_event("SHOCK", {"affected": ["AMD"]})))
    assert b._blocked_symbol("AMD") and not b._blocked_symbol("MSFT")
    t = seat(kill_dir)
    t.on_event("SHOCK", "shock", {"prices": {"AMD": 94.0}})
    assert t.signal.plans["AMD"].fair == pytest.approx(94.0)


# ═══ 5. Broker specifics: skew and widening ════════════════════════════
def test_broker_skew_shades_quotes_down_when_long_and_up_when_short():
    br = tb.MyBroker()
    br.spread("AMD", 100.0, [100.0] * 5)
    half = br._half_spread["AMD"]
    assert br.skew("AMD", 50) == pytest.approx(-0.5 * half)
    assert br.skew("AMD", tb.MAX_INVENTORY * 3) == pytest.approx(-half)   # capped
    assert br.skew("AMD", -50) == pytest.approx(0.5 * half)


def test_broker_widens_on_volatility_and_on_toxic_flow():
    import time
    br = tb.MyBroker()
    calm = br.spread("AMD", 100.0, [100.0] * 40)
    wild = br.spread("AMD", 100.0, [100.0 * (1.01 if i % 2 else 0.99) for i in range(40)])
    assert wild > calm
    br._toxic_until["AMD"] = time.monotonic() + 30
    assert br.spread("AMD", 100.0, [100.0] * 40) == pytest.approx(calm * tb.TOXIC_SPREAD_MULT)


# ═══ 6. The broker under the risk layer ════════════════════════════════
class _Sent(list):
    pass


def _event(name: str, data: dict):
    return type("E", (), {"event": name, "message": "", "data": data})()


def _broker(kill_dir, monkeypatch=None) -> tb.BudgetedBrokerBot:
    b = tb.BudgetedBrokerBot(tb.MyBroker())
    b._clock = TickClock(now=lambda: 0.0)
    b._clock.note(100)
    b.risk = RiskManager("stt_11_broker_1", BROKER_LIMITS, 300_000.0, kill_dir=kill_dir)
    b._session_open = True
    b._cash_est = 300_000.0
    b.state.exchange_prices.update({"AMD": 100.0, "MSFT": 400.0})
    b.risk.on_data(100)
    b.sent = _Sent()
    b.cancelled = _Sent()

    async def wire(msg):                                 # what reaches the venue
        b.sent.append(msg)

    async def cancel_all():
        b.cancelled.append("ALL")

    async def cancel(sym):
        b.cancelled.append(sym)
    b._wire = wire
    b.cancel_all_quotes = cancel_all
    b.cancel_quotes = cancel
    b.quote_centre = lambda s: b.state.exchange_prices.get(s)
    return b


@pytest.fixture
def wire(monkeypatch):
    async def fake_send(self, msg):
        await self._wire(msg)
    monkeypatch.setattr(tb._AdaptedBrokerBot, "_send", fake_send)


def _quote(sym="AMD", side="buy", qty=20, px=99.8) -> PlaceOrder:
    return PlaceOrder(team_id="stt_11_broker_1", symbol=sym, side=side,
                      order_type="limit", price=px, quantity=qty)


def test_broker_kill_switch_pulls_every_quote_and_flattens(kill_dir, wire):
    b = _broker(kill_dir)
    b.state.positions["AMD"] = 120
    b._cash_est -= 12_000
    asyncio.run(b._send(_quote()))
    assert len(b.sent) == 1                               # quoting normally
    (kill_dir / "KILL").write_text("flatten")
    b._clock.note(101)
    assert asyncio.run(b._run_risk()) is True
    assert b.cancelled[0] == "ALL"
    ioc = b.sent[-1]
    assert (ioc.order_type, ioc.side, ioc.quantity) == ("ioc", "sell", 120)
    asyncio.run(b._send(_quote()))
    assert b.sent[-1] is ioc                              # new quotes are refused


def test_broker_position_stop_closes_the_inventory(kill_dir, wire):
    b = _broker(kill_dir)
    b.state.positions["MSFT"] = 100
    b._cash_est -= 40_000
    b.risk.on_fill("MSFT", "buy", 100, 400.0)
    asyncio.run(b._run_risk())
    assert not b.sent
    b.state.exchange_prices["MSFT"] = 391.0               # −2.25%: past the 2% broker stop
    b._clock.note(101)
    asyncio.run(b._run_risk())
    ioc = b.sent[-1]
    assert (ioc.symbol, ioc.side, ioc.order_type) == ("MSFT", "sell", "ioc")
    b.state.positions["MSFT"] = 0
    asyncio.run(b._send(_quote("MSFT", "buy", 20, 390.0)))
    assert b.sent[-1] is ioc                              # cool-down: no bid in MSFT


def test_broker_gross_cap_cuts_the_quote(kill_dir, wire):
    b = _broker(kill_dir)
    b.state.positions["MSFT"] = 1000                      # $400k gross on $300k equity
    b._cash_est = 300_000.0 - 400_000.0
    asyncio.run(b._send(_quote("AMD", "buy", 20)))
    assert not b.sent


# ═══ 7. Exchange: rate limits, fat fingers, halt policy ═════════════════
def _venue(clock: Clock) -> MyExchange:
    ex = MyExchange()
    ex.guard = VenueGuard(VenueLimits(), clock=clock)
    return ex


def _order(team="team_a", sym="AMD", side="buy", px=100.0, qty=10, kind="limit"):
    return PlaceOrder(team_id=team, symbol=sym, side=side, order_type=kind,
                      price=px, quantity=qty)


def test_venue_normal_trader_is_never_rate_limited():
    clock = Clock()
    ex = _venue(clock)
    ex.on_trade("AMD", 100.0, 10, "x", "y")
    for _ in range(600):                                  # 10 orders/s for a minute
        clock.t += 0.1
        assert ex.accept_order(_order(), None) == (True, "")


def test_venue_abusive_flow_is_rate_limited_then_boxed():
    clock = Clock()
    ex = _venue(clock)
    results = [ex.accept_order(_order(), None)[0] for _ in range(70)]   # 70 in one instant
    assert results[:60] == [True] * 60 and not any(results[60:])
    assert "penalty box" in ex.accept_order(_order(), None)[1]
    assert ex.accept_order(_order(team="team_b"), None)[0]              # others unaffected
    clock.t += VenueLimits().penalty_ticks + 0.1
    assert ex.accept_order(_order(), None)[0]                           # served its time


def test_venue_house_bots_are_exempt(monkeypatch):
    import exchange.config as cfg
    monkeypatch.setattr(cfg, "HOUSE_TEAM_IDS", {"house_mm"})
    ex = _venue(Clock())
    assert all(ex.accept_order(_order(team="house_mm"), None)[0] for _ in range(200))


def test_venue_fat_finger_collar_and_notional_cap():
    ex = _venue(Clock())
    ex.on_trade("AMD", 100.0, 10, "x", "y")
    assert ex.accept_order(_order(px=109.0), None)[0]
    ok, why = ex.accept_order(_order(px=111.0), None)
    assert not ok and "through the last trade" in why
    assert ex.accept_order(_order(side="sell", px=91.0), None)[0]
    ok, why = ex.accept_order(_order(px=100.0, qty=3000), None)
    assert not ok and "notional" in why


def test_venue_halt_policy_refuses_then_reopens_with_a_tight_collar():
    clock = Clock()
    ex = _venue(clock)
    ex.on_trade("AMD", 100.0, 10, "x", "y")
    ex.on_event("TRADING_HALT", {"symbol": "AMD"})
    ok, why = ex.accept_order(_order(), None)
    assert not ok and "halted" in why
    assert ex.accept_order(_order(sym="MSFT"), None)[0]          # other symbols trade
    clock.t += 300
    ex.on_event("TRADING_RESUMED", {"symbol": "AMD"})
    assert not ex.accept_order(_order(px=104.0), None)[0]       # 4% > 3% reopen collar
    assert not ex.accept_order(_order(kind="market"), None)[0]
    assert ex.accept_order(_order(px=102.0), None)[0]
    clock.t += VenueLimits().reopen_ticks + 1
    assert ex.accept_order(_order(px=104.0), None)[0]           # normal collar again


def test_venue_market_halt_blocks_everything_until_resumed():
    clock = Clock()
    ex = _venue(clock)
    ex.on_event("MARKET_HALT", {"level": 1})
    assert not ex.accept_order(_order(sym="MSFT"), None)[0]
    ex.on_event("MARKET_RESUMED", {"level": 1})
    assert ex.accept_order(_order(sym="MSFT"), None)[0]


# ═══ 8. The kill switch object itself ═══════════════════════════════════
def test_kill_switch_holds_several_causes_and_logs_each(kill_dir):
    k = KillSwitch("s", kill_dir)
    k.trip("reject_storm", 10, until=70)
    k.trip("manual", 20, flatten=True)
    assert k.flatten(30) and k.reasons() == ["manual", "reject_storm"]
    assert k.active(80) and k.reasons() == ["manual"]   # the storm timed out
    k.reset("manual", 90)
    assert not k.active(90)
    assert [h[1:] for h in k.history] == [("trip", "reject_storm"), ("trip", "manual"),
                                          ("release", "reject_storm"), ("release", "manual")]


# ═══ 9. Regressions found by the stress session ════════════════════════
def test_ipo_allocation_before_listing_is_not_a_session_loss(kill_dir):
    """Phase3/results/stress_seed7 (first run): at IPO pricing the venue takes the
    cash but the new symbol has no mark until it lists; valued at $0 it read as a
    −10% session and tripped the hard stop. It is valued at the offer now."""
    t = seat(kill_dir)
    m = FakeMarket(AMD=(99.9, 100.1))
    t.on_tick(m, t.portfolio)                            # baseline $100k
    t.on_event("IPO_PRICED", "priced", {"symbol": "LUNA", "offer_price": 34.0})
    t.portfolio.positions["LUNA"] = 300                  # allocation: $10.2k paid
    t.portfolio.cash -= 300 * 34.0
    advance(t, 101)
    t.on_tick(m, t.portfolio)
    assert not t.risk.session.soft and not t.risk.kill.active(t.clock.now())


def test_a_position_with_no_price_at_all_skips_the_session_check(kill_dir):
    r = manager(kill_dir)
    r.update(100, 100_000.0, {}, {})
    r.update(101, 80_000.0, {"NEW": 500}, {})            # no mark, no entry: unknowable
    assert not r.session.soft


def test_stop_exit_after_a_gap_is_priced_inside_the_luld_band(kill_dir):
    """Phase3/results/stress_seed7 (before the fix): AMD gapped −9%, the position
    stop priced its exit off the stale bid (164.65) above the band around the
    new mark (150.45) and drew a PRICE_BAND reject. Now: priced off the mark,
    and the gate clamps into both edges of the band."""
    t = seat(kill_dir, cash=100_000.0 - 300 * 165.0, AMD=300)
    t.on_fill("buy", "AMD", 300, 165.0)
    advance(t, 101)
    t.on_book("AMD", 150.45)
    stale = FakeMarket(AMD=(164.65, 165.0))
    order = t.gate(t.on_tick(stale, t.portfolio), stale, t.portfolio)
    assert order.side == "sell" and order.price <= 150.45 * (1 + 0.08)
    assert t.gate(sell(qty=10, px=170.0), stale, t.portfolio).price <= 150.45 * 1.08


def test_a_position_stop_ends_the_signal_plan_instead_of_fighting_it(kill_dir):
    """Phase3/results/stress_seed23 (before the fix): the seat held +20 TSLA into a
    print, the stop sold it, and the signal's exit (back to +20) retried the buy
    41 times against the cool-down. The stop now ends the plan in that symbol."""
    t = seat(kill_dir, cash=100_000.0 - 20 * 700.0, NFLX=20)
    t.on_fill("buy", "NFLX", 20, 700.0)
    t.on_event("CALENDAR_EVENT", "print", {"kind": "econ_print", "pct": -0.02,
                                           "affected": ["NFLX"], "tick": 100})
    assert "NFLX" in t.signal.plans
    advance(t, 101)
    t.on_book("NFLX", 670.0)                              # −4.3% on the +20
    m = FakeMarket(NFLX=(669.5, 670.5))
    order = t.on_tick(m, t.portfolio)
    assert (order.side, order.quantity) == ("sell", 20)
    assert "NFLX" not in t.signal.plans                  # the plan is over
    t.portfolio.positions["NFLX"] = 0
    for tick in range(102, 120):
        advance(t, tick)
        assert t.on_tick(m, t.portfolio) is None         # nobody tries to buy it back
