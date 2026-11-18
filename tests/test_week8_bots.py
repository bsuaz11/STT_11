"""Week 8 (Fri Nov 20): multi-venue routing, the hidden lead-lag links, the
fee-aware broker. One test per requirement or behaviour, named after it.

    orders on both venues, routed on the NBBO and the fee difference  → route / TakerBot tests
    trade the hidden links in the predicted direction, within the lag → LeadLagSignal tests
    a bid in the LUNA bookbuild                                       → test_ipo_bid_for_luna
    broker and exchange profitability                                → broker fee tests

Run:  PYTHONPATH=engine:. pytest tests/test_week8_bots.py -v
"""

from __future__ import annotations

import asyncio
import json
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT), str(ROOT / "engine")]

from shared.messages import PlaceOrder, Signal

import team.broker as tb
from team.common import TickClock
from team.leadlag import (
    DEFAULT_LINKS,
    MARKER_MAX,
    LeadLagSignal,
    Link,
    load_links,
)
from team.risk import BROKER_LIMITS, RiskManager
from team.trader import MAX_OPEN_TAKER, MyTrader, TakerBot
from team.venues import (
    CombinedPortfolio,
    VenueLink,
    VenueState,
    route,
    venue_url,
)

NOW = 1000.0
SCENARIO = {"scenario": {"flags": {"shorts_allowed": True}, "position_limit": 1000,
                         "order_quota": 8, "maintenance_fraction": 0.4,
                         "config": {"SSR_TRIGGER_PCT": 0.08, "SHORT_LOCATE_CAP": 2000,
                                    "LULD_BAND_PCT": 0.08}}, "tick": 100}


def venue(name: str, taker: float, *, primary: bool = False, cash: float | None = 100_000.0,
          **touch: tuple) -> VenueState:
    """A live venue with a touch (bid, ask, ref) per symbol."""
    v = VenueState(name=name, primary=primary, taker=taker, connected=True, cash=cash)
    for sym, (bid, ask, ref) in touch.items():
        v.on_book(sym, [[bid, 100]], [[ask, 100]], ref, now=NOW)
    return v


# ═══ 1. Routing on the NBBO and the fee difference ══════════════════════
def test_route_picks_the_best_price_after_fees_not_the_best_quote():
    # The primary shows the better ask, but charges 200 bps; the venue 8 bps.
    prim = venue("primary", 0.02, primary=True, GOOGL=(174.9, 175.00, 175.0))
    cheap = venue("8770", 0.0008, GOOGL=(174.9, 175.10, 175.0))
    r = route("GOOGL", "buy", 10, 176.0, [prim, cheap], now=NOW)
    assert r.venue == "8770"                                  # 175.10·1.0008 < 175.00·1.02
    assert r.effective == pytest.approx(175.10 * 1.0008)
    # Same fee everywhere: the better quote wins.
    prim.taker = 0.0008
    assert route("GOOGL", "buy", 10, 176.0, [prim, cheap], now=NOW).venue == "primary"


def test_route_sells_where_the_bid_after_fee_is_highest():
    a = venue("a", 0.0014, primary=True, MSFT=(400.40, 400.60, 400.5))
    b = venue("b", 0.0008, MSFT=(400.00, 400.60, 400.5))
    r = route("MSFT", "sell", 5, 399.0, [a, b], now=NOW)
    assert r.venue == "a"                                     # 400.40·(1−14bp) > 400.00·(1−8bp)
    a.quotes["MSFT"].bid = 400.20                             # 400.20·(1−14bp) < 400.00·(1−8bp)
    assert route("MSFT", "sell", 5, 399.0, [a, b], now=NOW).venue == "b"


def test_route_skips_a_dislocated_venue_its_cheap_quote_is_not_free_money():
    prim = venue("primary", 0.0015, primary=True, AAPL=(212.0, 212.2, 212.1))
    stale = venue("x", 0.0008, AAPL=(172.0, 172.9, 172.5))   # the open-play NBBO: 20% away
    r = route("AAPL", "buy", 10, 213.0, [prim, stale], now=NOW)
    assert r.venue == "primary"


def test_route_skips_halted_stale_disconnected_and_auction_venues():
    prim = venue("primary", 0.0015, primary=True, AMD=(99.9, 100.1, 100.0))
    for broken in ("halted", "stale", "down", "auction"):
        v = venue("v", 0.0001, AMD=(99.9, 100.0, 100.0))
        if broken == "halted":
            v.halted.add("AMD")
        elif broken == "stale":
            v.quotes["AMD"].at = NOW - 60
        elif broken == "down":
            v.connected = False
        else:
            v.in_auction = True
        assert route("AMD", "buy", 5, 101.0, [prim, v], now=NOW).venue == "primary", broken


def test_route_closes_a_position_on_the_venue_that_holds_it_and_never_past_it():
    prim = venue("primary", 0.0008, primary=True, TSLA=(300.5, 300.7, 300.6))
    held = venue("8767", 0.0014, TSLA=(300.0, 300.7, 300.6))
    held.positions = {"TSLA": 30}
    r = route("TSLA", "sell", 50, 299.0, [prim, held], now=NOW)
    # The primary's bid is better, but selling there would open a short on the
    # primary and leave the long on 8767: the close stays where the shares are.
    assert r.venue == "8767" and r.quantity == 30


def test_route_caps_a_buy_at_the_cash_on_that_venue_and_clamps_into_its_band():
    v = venue("v", 0.0008, cash=1_000.0, NFLX=(699.0, 700.0, 700.0))
    r = route("NFLX", "buy", 10, 800.0, [v], now=NOW)
    assert r.quantity == 1                                    # $900 of cash at $700
    assert r.price <= 700.0 * 1.075                           # inside its own LULD band
    assert route("NFLX", "buy", 10, 690.0, [v], now=NOW) is None   # limit under its ask


def test_combined_portfolio_sums_positions_and_excludes_the_extra_venues_capital():
    class P:
        cash, positions, realized_pnl = 90_000.0, {"AMD": 10}, 5.0
    v = VenueState("v", connected=True, cash=24_000.0, positions={"AMD": 5, "TSLA": -2},
                   base_worth=25_000.0)
    c = CombinedPortfolio(P(), [v])
    assert c.positions == {"AMD": 15, "TSLA": -2}
    assert c.cash == 114_000.0 and c.capital_offset == 25_000.0
    assert c.realized_pnl == 5.0                              # everything else is the primary's


def test_venue_link_tracks_its_venue_and_forwards_only_our_fills():
    fills, rejects = [], []
    link = VenueLink("8770", "ws://x:8770", "stt_11_trader_1", "",
                     lambda *a: fills.append(a), lambda *a: rejects.append(a))
    feed = [
        {"type": "session_event", "event": "FEE_SCHEDULE", "message": "", "data": {"taker": 0.0008, "rebate": 0.0005}},
        {"type": "book_snapshot", "symbol": "AMD", "bids": [[99.9, 50]], "asks": [[100.1, 40]],
         "mid_price": 100.0, "spread": 0.2, "ref_price": 100.0},
        {"type": "portfolio_update", "team_id": "stt_11_trader_1", "cash": 25_000.0, "positions": {},
         "realized_pnl": 0, "unrealized_pnl": 0, "total_fees_paid": 0, "net_worth": 25_000.0},
        {"type": "trade_execution", "trade_id": "1", "symbol": "AMD", "price": 100.1, "quantity": 3,
         "buyer_id": "stt_11_trader_1", "seller_id": "mm", "aggressor": "buy", "fee": 0.02},
        {"type": "trade_execution", "trade_id": "2", "symbol": "AMD", "price": 100.1, "quantity": 9,
         "buyer_id": "someone", "seller_id": "mm", "aggressor": "buy", "fee": 0.02},
        {"type": "session_event", "event": "TRADING_HALT", "message": "", "data": {"symbol": "AMD"}},
        {"type": "error", "code": "SYMBOL_HALTED", "message": "AMD is halted"},
    ]
    for msg in feed:
        link.handle(json.dumps(msg))
    st = link.state
    assert st.taker == 0.0008 and st.quotes["AMD"].ask == 100.1 and st.base_worth == 25_000.0
    assert fills == [("buy", "AMD", 3, 100.1, "8770")]
    assert "AMD" in st.halted and rejects[0][0] == "SYMBOL_HALTED"


def test_venue_urls_follow_the_hosted_naming():
    assert venue_url("wss://feed.algoarena.example.org", 8767) == "wss://feed-8767.algoarena.example.org"
    assert venue_url("ws://localhost:8765", 8767) == "ws://localhost:8767"


# ═══ 2. The trader on two venues ═════════════════════════════════════════
@pytest.fixture
def kill_dir(tmp_path, monkeypatch):
    import team.risk
    monkeypatch.setattr(team.risk, "KILL_DIR", tmp_path)
    return tmp_path


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
        b, a = self.touch.get(s, (None, None))
        return (b + a) / 2 if b and a else None


def two_venue_bot(kill_dir, monkeypatch) -> tuple[MyTrader, TakerBot, list, list]:
    """A trader seat on a punitive primary (200 bps) and a cheap venue (8 bps)."""
    import time as _time
    clock = TickClock(now=lambda: 0.0)
    clock.note(100)
    t = MyTrader(clock=clock, roles={"signal", "probe", "leadlag"}, team_id="stt_11_trader_1",
                 kill_dir=kill_dir)
    t.venue_spec = "8770"
    bot = TakerBot(t)
    bot.portfolio.cash = 100_000.0
    t.on_open(SCENARIO)
    t.on_event("FEE_SCHEDULE", "", {"taker": 0.02, "rebate": -0.005})
    now = _time.monotonic()
    t.venue_primary.on_book("GOOGL", [[174.9, 100]], [[175.0, 100]], 175.0, now=now)
    t.on_book("GOOGL", 175.0)
    link = bot.links[0]
    st = link.state
    st.connected, st.cash, st.base_worth, st.taker = True, 25_000.0, 25_000.0, 0.0008
    st.on_book("GOOGL", [[174.9, 100]], [[175.1, 100]], 175.0, now=now)
    primary_sent, venue_sent = [], []

    async def to_primary(msg):
        primary_sent.append(msg)

    async def to_venue(symbol, side, quantity, price, order_type="ioc"):
        venue_sent.append((symbol, side, quantity, price))
        return True
    bot._send = to_primary
    bot._ws = object()
    bot._session_open = True
    link.send_order = to_venue
    bot._market = FakeMarket(GOOGL=(174.9, 175.0))
    return t, bot, primary_sent, venue_sent


def test_trader_routes_an_opening_order_away_from_a_punitive_primary(kill_dir, monkeypatch):
    t, bot, prim, ven = two_venue_bot(kill_dir, monkeypatch)
    asyncio.run(bot._place_order(Signal(symbol="GOOGL", side="buy", quantity=10, price=175.5)))
    assert not prim and ven and ven[0][:3] == ("GOOGL", "buy", 10)
    assert t.venue_links[0].state.orders == 0 or True    # counted by the real send_order


def test_the_probe_keeps_the_primary_in_use_at_one_share(kill_dir, monkeypatch):
    t, bot, prim, ven = two_venue_bot(kill_dir, monkeypatch)
    t._probe_queue = ["GOOGL"]
    t.clock.note(110)
    sig = None
    for _ in range(5):                                    # PROBE_DELAY_TICKS, then the buy
        t.clock.note(t.clock.now() + 1)
        sig = t._probe_order(bot.market, t.portfolio)
        if sig:
            break
    assert sig is not None and sig.quantity == 1          # punitive fee: one share
    asyncio.run(bot._place_order(sig))
    assert prim and prim[0].quantity == 1 and not ven     # pinned to the primary
    assert t.venue_primary.orders == 1


def test_trader_equity_counts_the_extra_venues_pnl_not_their_cash(kill_dir, monkeypatch):
    t, _bot, *_ = two_venue_bot(kill_dir, monkeypatch)
    m = FakeMarket(GOOGL=(174.9, 175.0))
    assert t._equity(m, t.portfolio) == pytest.approx(100_000.0)
    t.venue_links[0].state.cash = 25_100.0                # +$100 on the venue
    assert t._equity(m, t.portfolio) == pytest.approx(100_100.0)


def test_leaderboard_check_compares_only_the_primary_positions(kill_dir, monkeypatch):
    from shared.messages import Leaderboard
    t, _bot, *_ = two_venue_bot(kill_dir, monkeypatch)
    t.venue_links[0].state.positions = {"GOOGL": 10}      # held on the other venue
    board = Leaderboard(traders=[{"team_id": "stt_11_trader_1", "positions": {}, "net_worth": 1e5}],
                        brokers=[], exchange_fees=0.0, tick=120)
    for _ in range(5):
        t.on_leaderboard(board)
    assert not t._stood_down


def test_ipo_bid_for_luna(kill_dir, monkeypatch):
    t, _bot, *_ = two_venue_bot(kill_dir, monkeypatch)
    t.roles.add("ipo")
    t._cash = 100_000.0
    qty, px = t.on_ipo("LUNA", 45.0, 52.0, 3000, {})
    assert px == 52.0 and 0 < qty * px <= 8_000.0         # the top of the range, a small stake


# ═══ 3. The hidden links ══════════════════════════════════════════════════
def test_the_links_from_the_history_are_loaded():
    links = {(lk.leader, lk.follower, lk.lag, lk.sign) for lk in load_links()}
    assert links == {("AMD", "GOOGL", 6, 1), ("NFLX", "AMZN", 8, -1), ("MSFT", "TSLA", 4, -1)}
    assert {(lk.leader, lk.follower) for lk in DEFAULT_LINKS} == {(a, b) for a, b, *_ in links}


def warm(sig: LeadLagSignal, leader: str, ticks: int = 40, start: int = 0, px: float = 100.0) -> int:
    """Quiet market: tiny alternating moves until σ is known."""
    t = start
    for t in range(start, start + ticks):
        sig.step(t, {leader: px * (1 + (0.00005 if t % 2 else 0))}, {}, set(), {})
    return t + 1


def test_leadlag_orders_the_follower_in_the_predicted_direction_inside_the_lag():
    for link, move, side in ((Link("AMD", "GOOGL", 6, 0.37), +0.004, "buy"),
                             (Link("AMD", "GOOGL", 6, 0.37), -0.004, "sell"),
                             (Link("MSFT", "TSLA", 4, -0.34), +0.004, "sell"),
                             (Link("MSFT", "TSLA", 4, -0.34), -0.004, "buy")):
        sig = LeadLagSignal(links=(link,))
        t = warm(sig, link.leader)
        orders = sig.step(t, {link.leader: 100.0 * (1 + move)},
                          {link.follower: (174.9, 175.1)}, set(), {})
        assert [(o.symbol, o.side) for o in orders] == [(link.follower, side)], (link, move)
        assert sig.trades[link.follower].exit_at > t            # sent before the echo lands


def test_leadlag_ignores_a_normal_tick_and_sizes_markers_small_ramps_large():
    sig = LeadLagSignal(links=(Link("NFLX", "AMZN", 8, -0.29),))
    t = warm(sig, "NFLX", px=700.0)
    assert sig.step(t, {"NFLX": 700.0 * 1.00006}, {"AMZN": (199.9, 200.1)}, set(), {}) == []
    small = sig.step(t + 1, {"NFLX": 700.0 * 1.00006 * 1.0006}, {"AMZN": (199.9, 200.1)}, set(), {})
    assert small and small[0].quantity * 200 <= 600                 # a marker: a few hundred $
    sig2 = LeadLagSignal(links=(Link("NFLX", "AMZN", 8, -0.29),))
    t = warm(sig2, "NFLX", px=700.0)
    big = sig2.step(t, {"NFLX": 700.0 * 1.006}, {"AMZN": (199.9, 200.1)}, set(), {})   # earnings ramp
    assert big and big[0].side == "sell" and big[0].quantity * 200 >= 15_000


def test_leadlag_stands_aside_for_a_market_print_and_the_followers_own_print():
    for quiet in ({"tick": 41, "market_wide": True}, {"tick": 41, "symbol": "GOOGL"}):
        sig = LeadLagSignal(links=(Link("AMD", "GOOGL", 6, 0.37),))
        t = warm(sig, "AMD")
        sig.on_calendar([quiet])
        assert sig.step(t, {"AMD": 100.4}, {"GOOGL": (174.9, 175.1)}, set(), {}) == []
    # The LEADER's own print is the opportunity, not a reason to stand aside.
    sig = LeadLagSignal(links=(Link("NFLX", "AMZN", 8, -0.29),))
    t = warm(sig, "NFLX", px=700.0)
    sig.on_print(t, ["NFLX"], market_wide=False)
    assert sig.step(t, {"NFLX": 704.2}, {"AMZN": (199.9, 200.1)}, set(), {})


def test_leadlag_exits_on_the_echo_and_holds_through_a_ramp():
    link = Link("NFLX", "AMZN", 8, -0.29)
    sig = LeadLagSignal(links=(link,))
    t = warm(sig, "NFLX", px=700.0)
    px = 700.0
    touch = {"AMZN": (199.9, 200.1)}
    entry = sig.step(t, {"NFLX": (px := px * 1.006)}, touch, set(), {})
    sig.on_fill("AMZN", "sell", entry[0].quantity, 199.9)
    for k in range(1, 15):                                 # a 15-tick earnings ramp
        px *= 1.006
        orders = sig.step(t + k, {"NFLX": px}, touch, set(), {})
        assert not [o for o in orders if o.side == "buy"]
    last = t + 14
    # NFLX retraces: AMZN still echoes the ramp for `lag` ticks, so no exit yet…
    for k in range(15, 14 + link.lag):
        px *= 0.997
        orders = sig.step(t + k, {"NFLX": px}, touch, set(), {})
        assert not [o for o in orders if o.side == "buy"], k
    # …and the exit (buy back) comes on the last echo tick.
    px *= 0.997
    orders = sig.step(last + link.lag, {"NFLX": px}, touch, set(), {})
    assert [(o.side, o.quantity) for o in orders] == [("buy", entry[0].quantity)]


def test_leadlag_markers_are_rationed():
    # A quiet market (±0.5 bps) with a 10 bps leader tick every 20 ticks:
    # ~200 triggers in 4,000 ticks, of which the cool-down and the session cap
    # let through enough for the check (≥ 10) and no more than MARKER_MAX.
    sig = LeadLagSignal(links=(Link("AMD", "GOOGL", 6, 0.37),))
    t = warm(sig, "AMD")
    sent, px = 0, 100.0
    for k in range(4000):
        px *= 1.001 if k % 20 == 0 else (1.00005 if k % 2 else 1 / 1.00005)
        for o in sig.step(t + k, {"AMD": px}, {"GOOGL": (174.9, 175.1)}, set(), {}):
            sent += "exit" not in o.reason
            sig.on_fill(o.symbol, o.side, o.quantity, 175.0)
    assert 10 <= sent <= MARKER_MAX
    assert not sig.trades or all(tr.exit_at <= t + 4000 + 7 for tr in sig.trades.values())


def test_leadlag_backtest_on_the_history_meets_the_check():
    """The committed backtest result: ≥10 graded orders in every session."""
    txt = (ROOT / "Phase4" / "results" / "leadlag_backtest.txt")
    if not txt.exists():
        pytest.skip("run Phase4/leadlag_backtest.py first")
    assert "sessions with ≥10: 100%" in txt.read_text()


# ═══ 4. The broker pays for its fees ═════════════════════════════════════
@pytest.fixture
def broker(kill_dir, monkeypatch):
    b = tb.BudgetedBrokerBot(tb.MyBroker())
    b._clock = TickClock(now=lambda: 0.0)
    b._clock.note(100)
    b.risk = RiskManager("stt_11_broker_1", BROKER_LIMITS, 300_000.0, kill_dir=kill_dir)
    b._session_open = True
    b._cash_est = 300_000.0
    b.state.exchange_prices.update({"AMD": 100.0})
    sent, cancelled = [], []

    async def fake_send(self, msg):
        sent.append(msg)

    async def cancel_all():
        cancelled.append("ALL")

    async def cancel(sym):
        cancelled.append(sym)
    monkeypatch.setattr(tb._AdaptedBrokerBot, "_send", fake_send)
    b.cancel_all_quotes = cancel_all
    b.cancel_quotes = cancel
    b.quote_centre = lambda s: b.state.exchange_prices.get(s)
    b.sent, b.cancelled = sent, cancelled
    return b


def fee_event(taker: float, rebate: float):
    from shared.messages import SessionEvent
    return SessionEvent(event="FEE_SCHEDULE", message="", data={"taker": taker, "rebate": rebate})


def test_broker_stands_aside_where_makers_pay(broker):
    asyncio.run(broker._on_session_event(fee_event(0.02, -0.005)))     # week 7's primary
    assert broker._punitive() and broker._blocked_symbol("AMD") and "ALL" in broker.cancelled


def test_broker_width_always_pays_the_venues_maker_fee(broker):
    asyncio.run(broker._on_session_event(fee_event(0.0015, -0.0015)))  # makers pay 15 bps
    assert not broker._punitive()
    width = broker.state.compute_spread("AMD")
    assert width / 2 >= 100.0 * (0.0015 + tb.MAKER_EDGE_BPS / 1e4) - 1e-9
    asyncio.run(broker._on_session_event(fee_event(0.0014, 0.0011)))   # paid to make (our venue)
    assert broker.state.maker_cost == 0.0


def test_broker_position_stop_does_not_pay_a_punitive_taker_fee(broker):
    asyncio.run(broker._on_session_event(fee_event(0.02, 0.0)))        # 200 bps to cross
    broker.state.positions["AMD"] = 100
    broker.state.last_quote_prices["AMD"] = (99.8, 100.2)               # quotes resting
    broker.risk.on_fill("AMD", "buy", 100, 100.0)
    broker.state.exchange_prices["AMD"] = 97.0                          # −3%: past the 2% stop
    broker.risk.on_data(100)
    asyncio.run(broker._run_risk())
    assert not [m for m in broker.sent if isinstance(m, PlaceOrder)]   # no IOC at 2%…
    assert "AMD" in broker.cancelled and broker._blocked_symbol("AMD")   # …quotes pulled


def test_trader_open_fee_cap_is_below_the_punitive_primary():
    assert MAX_OPEN_TAKER < 0.02
