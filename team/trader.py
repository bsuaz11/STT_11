"""
STT_11 — trader seats (market taking).

Every seat runs this file; TEAM_ID picks what it does (SEAT_ROLES below).

    signal   Phase2/event_gap.py: take stale liquidity after a public print
    probe    a small round trip after the open and after every halt resumes
    ipo      bid in IPO bookbuilds and manage the allocation once it lists
    leadlag  trade the week-8 hidden links (team/leadlag.py): after a big
             leader move, an order on its follower before the echo lands

Orders go out as IOC. Whatever a role wants passes through gate() in
TakerBot._place_order, the one place orders leave the bot, so a role cannot
break a hard limit however it is written. Then route() (team/venues.py)
picks the venue: the best price after each venue's taker fee, among the
venues that are live and coherent with the primary (week 8: NBBO routing
and fee wars). TRADER_VENUES=auto|none|<url,…> picks the extra venues. The risk layer (team/risk.py: kill
switch, session and position stops, notional caps, halt re-entry) runs at the
top of every tick and inside gate(). Every timer runs on the exchange's
logical clock (team.common.TickClock), never the wall clock, so the same code
runs live, in the simulator and at any replay speed.

Run one seat:
    TEAM_ID=stt_11_trader_1 python -m team.trader
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import pathlib
import statistics
import time
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

import trader.config as _trader_config
from shared.messages import IPOSubscribe, OrderAck, PlaceOrder, Signal

from arena import Trader
from arena.trader import _AdaptedTraderBot
from Phase2.event_gap import EventGapSignal, GapParams, Plan
from team.common import (
    HaltTracker,
    HotPathMeter,
    TickClock,
    capital_baseline,
    short_interest,
    single_instance,
    team_positions,
)
from team.config import CAPITAL
from team.leadlag import LeadLagSignal
from team.risk import TRADER_LIMITS, RiskManager
from team.venues import (
    MAX_SECONDARY,
    CombinedPortfolio,
    Route,
    VenueLink,
    VenueState,
    discover_venues,
    route,
)

if TYPE_CHECKING:
    from shared.messages import BookSnapshot, Leaderboard, PortfolioUpdate, SessionEvent
    from trader.trader import MarketData, Portfolio

logger = logging.getLogger(__name__)

# ── Seats ────────────────────────────────────────────────────────────────
SEAT_ROLES = {
    "stt_11_trader_1": {"signal", "probe", "ipo", "leadlag"},
    "stt_11_trader_2": {"signal", "probe", "ipo", "leadlag"},
}
# Seats sharing the team's room in one symbol. Week 7 runs trader_1 alone;
# set SIGNAL_SEATS=2 in the shell if trader_2 trades too.
SIGNAL_SEATS = max(1, int(os.environ.get("SIGNAL_SEATS", "1")))
SKIP_SYMBOLS = ("ARENA10",)   # futures are locked until week 9

# ── Hard limits (scenario values replace these at SESSION_OPEN) ──────────
# Both the seat and team caps count every seat: the venue's limit is per TEAM.
# team/broker.py holds at most BROKER_MAX_SHARES either way in a symbol it
# quotes; keep them in sync. The live block runs ONE broker seat.
POSITION_LIMIT = 1000
POSITION_BUFFER = 20          # stay this far under the limit
BROKER_MAX_SHARES = 150
MAX_ORDER_SHARES = 500        # venue rejects anything larger
# Gross and net notional caps live in team/risk.py (TRADER_LIMITS).
MAINTENANCE_FRACTION = 0.4    # the venue liquidates below this × capital
RISK_OFF_MARGIN = 0.20        # reduce-only this far above that line
LULD_BAND_PCT = 0.08
LULD_MARGIN_PCT = 0.005       # keep limit prices this far inside the band
CASH_USE = 0.90               # spend at most this share of cash on one order

# ── Shorting (a short that hits the bid under SSR is rejected) ───────────
SSR_TRIGGER_PCT = 0.06
SSR_MARGIN_PCT = 0.01         # stop opening shorts this far before the trigger
SHORT_LOCATE_CAP = 2000
BORROW_HEADROOM = 300
SHORT_REJECTS = ("SHORTS_LOCKED", "SSR_RESTRICTED", "BORROW_UNAVAILABLE")

# ── Execution ────────────────────────────────────────────────────────────
PENDING_TICKS = 1.0           # an IOC with no fill by now is dead
FILL_SETTLE_TICKS = 0.15      # after a fill, wait this long for the rest of the IOC
SLIP_BPS = 20                 # how far through the touch a probe or stop IOC reaches
MIN_SHOCK_PCT = 0.005         # ignore teacher SHOCK moves smaller than this

# ── Probe: visible activity after the open and after every halt ──────────
PROBE_SHARES = 5
PROBE_SYMBOL = "MSFT"
PROBE_HOLD_TICKS = 10
PROBE_DELAY_TICKS = 3         # let the book refill after a resume
# Unfilled buys before the probe gives up. Our own broker's ask is often the
# touch; the venue's self-trade prevention then cancels it instead of filling
# us, and retrying every tick churns the team's quota and cancel fees.
PROBE_MAX_BUYS = 3

# ── Venues (week 8: NBBO routing, fee wars) ─────────────────────────────
# An order that OPENS exposure only goes to a venue charging at most this
# taker fee: the primary charged 200 bps in week 7 (400 in open play since),
# which no strategy here can pay. Closes go wherever the position is.
MAX_OPEN_TAKER = 0.0060
VENUE_MSGS_PER_SEC = 4        # per extra venue (each venue meters its own quota)
# The probe keeps the primary in use (orders on both venues) at minimum cost:
# one share while the primary's fee is punitive.
PROBE_PUNITIVE_SHARES = 1

# ── IPO ──────────────────────────────────────────────────────────────────
# Each seat bids this share of its cash at the top of the range (the sure
# allocation). The allocation can only be sold on the primary, at its fee
# (200 bps in week 7), so the stake is kept small: $8k at LUNA's $52 top is
# ~150 shares; a deal that breaks 10% costs under 1% of the seat.
IPO_CASH_SHARE = 0.08
IPO_MAX_SHARES = 400
IPO_RETRY_TICKS = 5           # resend an indication with no confirmation
IPO_MAX_TRIES = 6
# Listing exit. Calibrated by reasoning, not data: one recorded listing exists.
LISTING_STOP_PCT = 0.04       # sell if it breaks this far below the offer
LISTING_TRAIL_PCT = 0.03      # or falls this far from its high, once in profit
LISTING_MIN_GAIN_PCT = 0.03
LISTING_MAX_HOLD_TICKS = 1200

# ── Flashes (week 7+) ────────────────────────────────────────────────────
# A FLASH is an unannounced 1.5–2.5% jump in one symbol on this venue that
# walks back within `revert_ticks` (~2 s). Fade it the moment the event lands:
# sell an up-flash / buy a down-flash with an IOC limited to fair ± edge, where
# fair is the pre-flash EWMA of the venue mark and edge covers a taker fee in
# and out plus a buffer. A flash too small for that edge still gets the order
# (it just doesn't fill): acting is graded, a losing fill is not worth taking.
# Exit with an IOC once the price is back near fair, or at the time stop.
FLASH_NOTIONAL = 25_000.0     # dollars per flash
FLASH_MAX_SHARES = 300
FLASH_BUFFER_BPS = 15         # profit required on top of two taker fees
FLASH_RETRY_FRACTION = 0.6    # re-try an unfilled entry only this early in the flash
FLASH_REVERTED = 0.25         # "reverted" once within this share of the jump from fair
FLASH_EXIT_GRACE_TICKS = 1.0  # exit anyway this long after the advertised revert
FLASH_MAX_HOLD_TICKS = 20     # then cross harder until flat
FLASH_EXIT_SLIP_BPS = 20
FLASH_PANIC_SLIP_BPS = 60
FLASH_SHORT_HEADROOM = 50     # shares left under the market-wide locate cap
FAIR_ALPHA = 0.05             # EWMA of the mark per snapshot (~4 s at 0.2 s)
FAIR_JUMP_PCT = 0.01          # a mark this far off fair is a flash: don't learn it…
FAIR_RESET_SNAPSHOTS = 25     # …unless it stays there this long (a real move)

# ── Message quota (week 7: 8 orders+cancels per tick, per TEAM) ──────────
# The broker keeps itself to quota − TRADER_RESERVE (3) per second; the trader
# stays inside that reserve. Flash orders may borrow from the venue's burst
# allowance (3× the quota), which the steady state leaves almost full.
TRADER_MSGS_PER_SEC = 2
TRADER_FLASH_BURST = 4        # flash orders may go up to this many per second
QUOTA_BACKOFF_TICKS = 1.0     # after a RATE_LIMITED, only flash orders for this long
STATS_EVERY_SEC = 60.0

STATE_DIR = pathlib.Path(__file__).resolve().parent.parent / "Phase2" / "state"
STATE_MAX_AGE_TICKS = 3600    # older than a block: it belongs to another session
# A seat whose own position view keeps disagreeing with the leaderboard is not
# hearing about its fills: another process (here or on a teammate's machine)
# holds the seat's connection. It stops trading rather than trade blind.
STAND_DOWN_BOARDS = 3         # consecutive mismatching leaderboards (~2 s apart)


class RoundTrip:
    """Buy a few shares, hold them briefly, sell them back."""

    def __init__(self, symbol: str, not_before: float = 0.0) -> None:
        self.symbol = symbol
        self.phase = "buy"
        self.held = 0
        self.wait_until = not_before
        self.buys = 0

    def on_fill(self, side: str, quantity: int, now: float) -> None:
        """Count a fill of the probe's own order and advance its phase."""
        if side == "buy":
            self.held += quantity
            if self.phase == "buy":
                self.phase, self.wait_until = "sell", now + PROBE_HOLD_TICKS
        else:
            self.held = max(0, self.held - quantity)
            if self.held == 0 and self.phase == "sell":
                self.phase = "done"


class FlashTrade:
    """One flash: fade it with an IOC, then buy/sell back once it reverts.

    phase  entry → hold → done   (entry with no fill goes straight to done)
    held   signed shares this trade has put on (negative after an up-flash)
    """

    def __init__(self, symbol: str, pct: float, fair: float, t0: float,
                 revert_ticks: float, qty: int, edge: float, wall: float) -> None:
        self.symbol, self.pct, self.fair, self.t0 = symbol, pct, fair, t0
        self.revert_ticks = revert_ticks
        self.side = "sell" if pct > 0 else "buy"          # fade the jump
        self.qty = qty
        # The IOC limit: only fills where the fade clears two fees + buffer.
        self.limit = (math.ceil(fair * (1 + edge) * 100) / 100 if pct > 0
                      else math.floor(fair * (1 - edge) * 100) / 100)
        self.phase = "entry"
        self.tries = 0
        self.held = 0
        self.cash = 0.0                                    # + received, − paid
        self.notional = 0.0
        self.wall = wall                                   # monotonic, at the event
        self.react_ms: float | None = None

    def on_fill(self, side: str, quantity: int, price: float) -> None:
        signed = quantity if side == "buy" else -quantity
        self.held += signed
        self.cash -= signed * price
        self.notional += quantity * price


class MyTrader(Trader):
    """Roles, risk and the hard limits. The signal itself is in Phase2/."""

    def __init__(self, clock: TickClock | None = None, roles: set[str] | None = None,
                 team_id: str | None = None, params: GapParams | None = None,
                 state_path: pathlib.Path | None = None,
                 kill_dir: pathlib.Path | None = None) -> None:
        super().__init__()
        self.team_id = team_id or _trader_config.TEAM_ID
        self.clock = clock or TickClock()
        self.halts = HaltTracker(self.clock)
        self.risk = RiskManager(self.team_id, TRADER_LIMITS,
                                float(CAPITAL.get(self.team_id, _trader_config.STARTING_CASH)),
                                kill_dir=kill_dir)
        self._closing_sig: Signal | None = None   # the order a stop or the kill switch wants
        env_roles = os.environ.get("TRADER_ROLES")
        self.roles = roles if roles is not None else (
            {r.strip() for r in env_roles.split(",") if r.strip()} if env_roles
            else SEAT_ROLES.get(self.team_id, set()))
        logger.info("Seat %s roles: %s", self.team_id,
                    ", ".join(sorted(self.roles)) or "NONE — this seat will not trade")
        self.signal = EventGapSignal(params=params or GapParams())
        self.leadlag = LeadLagSignal()
        self._ll_leaders = {lk.leader for lk in self.leadlag.links}
        self._ll_last_ref: dict[str, float] = {}  # leader -> last venue mark seen
        self._ll_rets: dict[str, float] = {}      # leader -> log return since the last step
        self._ll_queue: list = []                 # leadlag orders waiting for the quota
        self._ll_tick = -1
        self._event_t0: float | None = None       # server tick of the opening cross (t0)
        # Venues: the primary's state is kept here; TakerBot adds the others.
        self.venue_primary = VenueState("primary", primary=True)
        self.venue_links: list[VenueLink] = []
        self.venue_spec = "none"                  # MyTrader.run() reads TRADER_VENUES
        self._probe_sig: Signal | None = None
        self.portfolio = None                     # set by whatever runs this seat
        self._logged_plans = 0
        self._state_path = state_path             # survives a process restart mid-trade
        self._state_loaded = False
        # Rules of the session
        self._limit = POSITION_LIMIT
        self._maint = MAINTENANCE_FRACTION
        self._luld = LULD_BAND_PCT
        self._shorts_ok = False
        self._ssr_pct = SSR_TRIGGER_PCT
        self._locate_cap = SHORT_LOCATE_CAP
        self._capital: float | None = None        # set from the first leaderboard
        # Market state
        self._ref: dict[str, float] = {}          # symbol -> venue mark (ref_price)
        self._open_mid: dict[str, float] = {}     # symbol -> first mid after the open
        self._ssr: set[str] = set()
        self._short_interest: dict[str, int] = {}
        self._team_others: dict[str, int] = {}    # symbol -> other team seats' position
        self._no_short_until = 0.0
        self._in_auction = False                  # pre-open / pre-close: IOCs are refused
        self._liquidated = False
        self._risk_off = False
        self._view_mismatch = 0                   # consecutive boards disagreeing with us
        self._stood_down = False
        self._cash = 0.0
        # Order state
        self._pending: dict[str, float] = {}      # symbol -> tick its last order settles
        self._inflight: list[list] = []           # [dead by, symbol, side, unfilled qty]
        self._probe: RoundTrip | None = None
        self._probe_queue: list[str] = []
        # IPO state
        self._ipo: dict[str, dict] = {}           # symbol -> indication awaiting confirmation
        self._listings: dict[str, dict] = {}      # symbol -> {offer, high, t0}
        self._offer: dict[str, float] = {}        # priced IPO -> offer (its mark until it lists)
        # Flashes
        self._fair: dict[str, float] = {}         # symbol -> EWMA of the mark, jumps excluded
        self._fair_off: dict[str, int] = {}       # symbol -> snapshots in a row far from fair
        self._flash: dict[str, FlashTrade] = {}   # symbol -> live flash trade
        self.flash_log: list[dict] = []           # one row per flash seen this process
        # Quota accounting (this seat's share of the team's messages)
        self.msgs_sent = 0
        self.rate_limited = 0
        self._quota_backoff_until = 0.0
        self.hot = HotPathMeter()
        self._stats_at = time.monotonic()

    # ── Each tick ───────────────────────────────────────────────────────
    def on_tick(self, market: MarketData, portfolio: Portfolio) -> Signal | None:
        """Refresh state, then: risk off > flash > signal > listing exit > probe."""
        self._cash = portfolio.cash
        for sym in self.halts.expired():
            self._on_resumed(sym)
        if self._in_auction or self._liquidated or self._stood_down:
            return None
        for sym in market.symbols():
            if sym not in self._open_mid:
                mid = market.mid_price(sym)
                if mid:
                    self._open_mid[sym] = mid

        equity = self._equity(market, portfolio)
        decision = self.risk.update(self.clock.now(), equity, portfolio.positions,
                                    self._marks(market, portfolio))
        if decision.flatten_all:                  # kill switch with flatten
            return self._reduce_largest(market, portfolio, "KILL SWITCH flatten")
        if decision.kill:                         # kill switch: send nothing new
            return None
        for sym in decision.flatten:              # per-position stop
            sig = self._close(sym, market, portfolio, "POSITION STOP")
            if sig is not None:
                return sig
        self._risk_off = equity < self._risk_off_level()
        if self._risk_off:
            return self._reduce_largest(market, portfolio, "RISK OFF")
        return (self._flash_order(market, portfolio)
                or self._signal_order(market, portfolio)
                or self._leadlag_order(market, portfolio)
                or self._listing_exit(market, portfolio)
                or self._probe_order(market, portfolio))

    # ── Role: lead-lag (week 8) ─────────────────────────────────────────
    def _leadlag_order(self, market: MarketData, portfolio: Portfolio) -> Signal | None:
        """Once per tick: feed the leader returns to the signal; then send the
        next queued order (entries before the echo lands, exits on it)."""
        if "leadlag" not in self.roles:
            return None
        tick = int(self.clock.now())
        if tick != self._ll_tick:
            self._ll_tick = tick
            rets, self._ll_rets = self._ll_rets, {}
            fol = self.leadlag.followers
            touch = {s: self._best_touch(s, market) for s in fol}
            halted = {s for s in fol | self._ll_leaders if self.halts.is_halted(s)}
            halted |= {s for s in fol if self.risk.resume.blocks(s, self.clock.now(), *touch[s])}
            probe = self._probe.symbol if self._probe is not None and self._probe.phase != "done" else None
            busy = {s for s in fol if s in self.signal.plans or s in self._flash or s == probe
                    or s in self.risk.book.cooldown and self.risk.book.cooldown[s] > self.clock.now()}
            orders = self.leadlag.step(tick, self._ref, touch, halted, portfolio.positions,
                                       rets=rets, busy=busy)
            # Exits are re-issued every tick, so keep only the newest per symbol.
            fresh = {(o.symbol, o.side) for o in orders}
            self._ll_queue = [o for o in self._ll_queue if (o.symbol, o.side) not in fresh] + orders
        while self._ll_queue:
            o = self._ll_queue.pop(0)
            if self.halts.is_halted(o.symbol):
                continue
            if self._is_pending(o.symbol):
                self._ll_queue.insert(0, o)
                return None
            self._mark_pending(o.symbol)
            logger.info("LEADLAG %s: %s %d %s @ %.2f", o.reason, o.side, o.quantity, o.symbol, o.price)
            return Signal(symbol=o.symbol, side=o.side, quantity=o.quantity, price=o.price)
        return None

    def _best_touch(self, sym: str, market: MarketData) -> tuple[float | None, float | None]:
        """Best bid and ask across the venues an order could go to (the NBBO
        among live, coherent venues); the primary's touch if none."""
        bid, ask = market.best_bid(sym), market.best_ask(sym)
        ref = self._ref.get(sym)
        for link in self.venue_links:
            st = link.state
            t = st.quotes.get(sym)
            if t is None or not st.usable(sym) or st.taker > MAX_OPEN_TAKER:
                continue
            if ref and t.ref and abs(t.ref / ref - 1) > 0.01:
                continue
            if t.bid and (bid is None or t.bid > bid):
                bid = t.bid
            if t.ask and (ask is None or t.ask < ask):
                ask = t.ask
        return bid, ask

    def route_fee(self) -> float:
        """The taker fee an opening order will actually pay: the cheapest live
        venue under MAX_OPEN_TAKER, else the primary's."""
        fees = [lk.state.taker for lk in self.venue_links
                if lk.state.connected and lk.state.taker <= MAX_OPEN_TAKER]
        if self.venue_primary.taker <= MAX_OPEN_TAKER:
            fees.append(self.venue_primary.taker)
        return min(fees) if fees else self.venue_primary.taker

    # ── Role: flash ─────────────────────────────────────────────────────
    def _on_flash(self, data: dict) -> None:
        """A FLASH just hit this venue: set up the fade (sent by the next on_tick,
        which TakerBot runs at once)."""
        sym, pct = data.get("symbol"), data.get("pct")
        if not sym or not isinstance(pct, (int, float)) or not pct:
            return
        pct = float(pct)
        if abs(pct) > 1:                         # tolerate "2.0" meaning 2%
            pct /= 100
        row = {"symbol": sym, "pct": pct, "tick": data.get("tick"), "wall": time.monotonic(),
               "acted": False, "react_ms": None, "filled": 0, "pnl": 0.0, "why": ""}
        self.flash_log.append(row)
        # The venue sends its exact pre-flash fair value; our EWMA is the fallback.
        start = data.get("start_price")
        fair = (float(start) if isinstance(start, (int, float)) and start > 0
                else self._fair.get(sym) or self._ref.get(sym))
        why = ("no signal role" if "signal" not in self.roles else
               "skipped symbol" if sym in SKIP_SYMBOLS else
               "event trade open in it" if sym in self.signal.plans else
               "already fading it" if sym in self._flash else
               "no fair price yet" if not fair else
               "halted" if self.halts.is_halted(sym) else
               "auction" if self._in_auction else
               "risk off / liquidated / stood down" if (self._risk_off or self._liquidated
                                                         or self._stood_down) else "")
        if why:
            row["why"] = why
            logger.info("FLASH %s %+.2f%%: not fading (%s)", sym, pct * 100, why)
            return
        edge = 2 * self.signal.taker_fee + FLASH_BUFFER_BPS / 1e4
        qty = min(FLASH_MAX_SHARES, int(FLASH_NOTIONAL / fair))
        revert = float(data.get("revert_ticks") or 2)
        self._flash[sym] = FlashTrade(sym, pct, fair, self.clock.now(), revert, qty, edge,
                                      row["wall"])
        self._flash[sym].row = row
        logger.info("FLASH %s %+.2f%%: fair=%.2f, fading with %s %d @ %.2f (edge %.0f bps)",
                    sym, pct * 100, fair, self._flash[sym].side, qty,
                    self._flash[sym].limit, edge * 1e4)

    def _flash_room(self, ft: FlashTrade, portfolio: Portfolio) -> int:
        """Shares the fade may trade now: cash/limits for a buy; for a sell what
        is held plus a short the market-wide borrow can still cover."""
        pos = portfolio.positions.get(ft.symbol, 0)
        if ft.side == "buy":
            return max(0, min(ft.qty, self._seat_caps(ft.symbol)[0] - pos,
                              int(portfolio.cash * CASH_USE / ft.limit)))
        sell = max(0, pos)
        if (self._shorts_ok and ft.symbol not in self._ssr
                and self.clock.now() >= self._no_short_until):
            borrow = (self._locate_cap - self._short_interest.get(ft.symbol, 0)
                      - FLASH_SHORT_HEADROOM)
            short_cap = self._seat_caps(ft.symbol)[1] - max(0, -pos)
            sell += max(0, min(borrow, short_cap))
        return min(ft.qty, sell)

    def _flash_order(self, market: MarketData, portfolio: Portfolio) -> Signal | None:
        now = self.clock.now()
        for sym, ft in list(self._flash.items()):
            if self.halts.is_halted(sym):
                continue
            bid, ask = market.best_bid(sym), market.best_ask(sym)
            age = now - ft.t0
            if ft.phase == "entry":
                if self._is_pending(sym):
                    continue
                if ft.held:                       # the IOC filled: wait for the revert
                    ft.phase = "hold"
                elif ft.tries == 0 or (
                        ft.tries == 1 and age < ft.revert_ticks * FLASH_RETRY_FRACTION
                        and ((ft.side == "sell" and bid and bid >= ft.limit)
                             or (ft.side == "buy" and ask and ask <= ft.limit))):
                    qty = self._flash_room(ft, portfolio)
                    if qty <= 0:
                        self._flash_done(ft, "no room (limits, cash or borrow)")
                        continue
                    ft.tries += 1
                    self._mark_pending(sym)
                    return Signal(symbol=sym, side=ft.side, quantity=qty, price=ft.limit)
                else:
                    self._flash_done(ft, "no fill at the edge")
                    continue
            if ft.phase == "hold":
                if ft.held == 0:
                    self._flash_done(ft, "flat")
                    continue
                if self._is_pending(sym):
                    continue
                back = ft.fair * (1 + FLASH_REVERTED * ft.pct)
                reverted = ((ft.held < 0 and ask and ask <= back)
                            or (ft.held > 0 and bid and bid >= back))
                if not (reverted or age >= ft.revert_ticks + FLASH_EXIT_GRACE_TICKS):
                    continue
                slip = (FLASH_PANIC_SLIP_BPS if age >= FLASH_MAX_HOLD_TICKS
                        else FLASH_EXIT_SLIP_BPS) / 1e4
                self._mark_pending(sym)
                if ft.held < 0:
                    ref = ask or ft.fair
                    return Signal(symbol=sym, side="buy", quantity=min(-ft.held, MAX_ORDER_SHARES),
                                  price=round(ref * (1 + slip), 2))
                ref = bid or ft.fair
                return Signal(symbol=sym, side="sell", quantity=min(ft.held, MAX_ORDER_SHARES),
                              price=round(ref * (1 - slip), 2))
        return None

    def _flash_done(self, ft: FlashTrade, why: str) -> None:
        fees = ft.notional * self.signal.taker_fee
        pnl = ft.cash - fees if ft.held == 0 else 0.0
        row = getattr(ft, "row", {})
        row.update(why=why, pnl=pnl)
        self._flash.pop(ft.symbol, None)
        logger.info("FLASH %s done (%s): traded $%.0f, pnl %+.2f after ~%.2f fees",
                    ft.symbol, why, ft.notional, pnl, fees)

    def flash_summary(self) -> str:
        rows = self.flash_log
        acted = [r for r in rows if r["acted"]]
        rts = [r["react_ms"] for r in acted if r["react_ms"] is not None]
        filled = [r for r in rows if r["filled"]]
        s = f"flashes seen {len(rows)}, acted on {len(acted)}, filled {len(filled)}"
        if rts:
            s += f", reaction median {statistics.median(rts):.0f} ms / slowest {max(rts):.0f} ms"
        if filled:
            s += f", flash pnl {sum(r['pnl'] for r in filled):+.2f}"
        return s

    # ── Role: signal ────────────────────────────────────────────────────
    def _signal_order(self, market: MarketData, portfolio: Portfolio) -> Signal | None:
        tick = int(self.clock.now())
        for sym in list(self.signal.plans):
            if self.halts.is_halted(sym) or self._is_pending(sym):
                continue
            bid, ask = market.best_bid(sym), market.best_ask(sym)
            order = self.signal.decide(
                sym, tick, bid, ask, portfolio.positions.get(sym, 0),
                self._long_room(sym, portfolio, ask), self._short_room(sym, portfolio, bid))
            if order is not None:
                logger.info("EVENTGAP %s: %s %d %s @ %.2f (fair=%.2f t+%d)",
                            order.reason, order.side, order.quantity, sym, order.price,
                            self.signal.plans[sym].fair, tick - self.signal.plans[sym].t0)
                self._mark_pending(sym)
                return Signal(symbol=sym, side=order.side, quantity=order.quantity,
                              price=order.price)
        if len(self.signal.closed) > self._logged_plans:
            for result in self.signal.closed[self._logged_plans:]:
                if result.traded:
                    logger.info("EVENTGAP closed %s %+.2f%%: bought %d sold %d pnl=%+.2f fees=%.2f",
                                result.symbol, result.pct * 100, result.bought, result.sold,
                                result.pnl, result.fees)
            self._logged_plans = len(self.signal.closed)
            self._save_state()
        return None

    def _on_print(self, sym: str, pct: float) -> None:
        if "signal" not in self.roles or sym in SKIP_SYMBOLS:
            return
        self.signal.set_fee(self.route_fee())
        plan = self.signal.on_print(sym, pct, self._ref.get(sym, 0.0), int(self.clock.now()),
                                    self.portfolio.positions.get(sym, 0))
        if plan:
            logger.info("EVENTGAP print %s %+.2f%%: pre_mark=%.2f fair=%.2f edge=%.0f bps",
                        sym, pct * 100, plan.pre_mark, plan.fair, self.signal.edge * 1e4)
            self._save_state()

    def _long_room(self, sym: str, portfolio: Portfolio, ask: float | None) -> int:
        room = self._seat_caps(sym)[0] - portfolio.positions.get(sym, 0)
        return min(room, int(portfolio.cash * CASH_USE / ask)) if ask else 0

    def _short_room(self, sym: str, portfolio: Portfolio, bid: float | None) -> int:
        pos = portfolio.positions.get(sym, 0)
        if self._may_short(sym, bid):
            return pos + self._seat_caps(sym)[1]
        return max(0, pos)

    def _may_short(self, sym: str, bid: float | None) -> bool:
        """Would a new short get past the venue? Not under (or about to trip)
        the short-sale rule, and borrow is left."""
        if (not self._shorts_ok or sym in self._ssr or bid is None
                or self.clock.now() < self._no_short_until):
            return False
        if (self._short_interest.get(sym, 0) + self._seat_caps(sym)[1] + BORROW_HEADROOM
                > self._locate_cap):
            return False
        open_mid = self._open_mid.get(sym)
        return not open_mid or bid > open_mid * (1 - self._ssr_pct + SSR_MARGIN_PCT)

    # ── Role: probe ─────────────────────────────────────────────────────
    def _on_resumed(self, sym: str) -> None:
        logger.info("RESUMED %s — re-entering only once the book settles", sym)
        self.risk.on_resume(sym, self.clock.now())
        if "probe" in self.roles and sym not in SKIP_SYMBOLS and sym not in self._probe_queue:
            self._probe_queue.append(sym)

    def _probe_order(self, market: MarketData, portfolio: Portfolio) -> Signal | None:
        now = self.clock.now()
        if self._probe is None or self._probe.phase == "done":
            # Never in a symbol the signal is trading: its fills would count there.
            ready = [s for s in self._probe_queue
                     if s not in self.signal.plans and s not in self._flash
                     and not self.halts.is_halted(s)]
            if not ready:
                return None
            self._probe_queue.remove(ready[0])
            self._probe = RoundTrip(ready[0], now + PROBE_DELAY_TICKS)
            logger.info("Probe: round trip of %d %s", PROBE_SHARES, ready[0])
        p = self._probe
        bid, ask = market.best_bid(p.symbol), market.best_ask(p.symbol)
        if (now < p.wait_until or self.halts.is_halted(p.symbol) or self._is_pending(p.symbol)
                or p.symbol in self.signal.plans or p.symbol in self._flash):
            return None
        if p.phase == "buy" and ask:
            if p.buys >= PROBE_MAX_BUYS:
                logger.info("Probe %s: no fill after %d tries — dropping it", p.symbol, p.buys)
                p.phase = "done"
                return None
            p.buys += 1
            px = round(ask * (1 + SLIP_BPS / 1e4), 2)
            shares = (PROBE_PUNITIVE_SHARES if self.venue_primary.taker > MAX_OPEN_TAKER
                      else PROBE_SHARES)
            if not portfolio.can_buy(p.symbol, shares, px):
                p.phase = "done"
                return None
            self._mark_pending(p.symbol)
            self._probe_sig = Signal(symbol=p.symbol, side="buy", quantity=shares, price=px)
            return self._probe_sig
        if p.phase == "sell" and bid and p.held > 0:
            self._mark_pending(p.symbol)
            self._probe_sig = Signal(symbol=p.symbol, side="sell", quantity=p.held,
                                     price=round(bid * (1 - SLIP_BPS / 1e4), 2))
            return self._probe_sig
        return None

    def venue_for(self, sig: Signal) -> str | None:
        """A venue this order must use, or None to route it. The probe is the
        primary's heartbeat (and keeps both venues in use) — the primary."""
        return "primary" if sig is self._probe_sig else None

    # ── Role: IPO ───────────────────────────────────────────────────────
    def on_ipo(self, symbol: str, lo: float, hi: float, shares: int,
               data: dict) -> tuple[int, float] | None:
        """Bid the top of the range. pending_ipo() retries until confirmed."""
        if "ipo" not in self.roles or hi <= 0:
            return None
        cash = self._cash or _trader_config.STARTING_CASH   # may fire before the first tick
        qty = min(shares, IPO_MAX_SHARES, int(cash * IPO_CASH_SHARE / hi))
        if qty <= 0:
            logger.warning("IPO %s: cash %.2f can't fund one share at %.2f", symbol, cash, hi)
            return None
        self._ipo[symbol] = {"qty": qty, "px": hi, "tries": 1,
                             "next": self.clock.now() + IPO_RETRY_TICKS}
        logger.info("IPO %s: bidding %d at %.2f", symbol, qty, hi)
        return qty, hi

    def pending_ipo(self) -> IPOSubscribe | None:
        """The next indication to (re)send, or None. Called by TakerBot."""
        now = self.clock.now()
        for sym, ind in self._ipo.items():
            if now < ind["next"] or ind["tries"] >= IPO_MAX_TRIES:
                continue
            ind["next"], ind["tries"] = now + IPO_RETRY_TICKS, ind["tries"] + 1
            logger.warning("IPO %s: unconfirmed, resending %d (try %d)", sym, ind["qty"], ind["tries"])
            return IPOSubscribe(team_id=self.team_id, symbol=sym,
                                quantity=ind["qty"], max_price=ind["px"])
        return None

    def _listing_exit(self, market: MarketData, portfolio: Portfolio) -> Signal | None:
        now = self.clock.now()
        for sym, deal in list(self._listings.items()):
            pos = portfolio.positions.get(sym, 0)
            mark = self._ref.get(sym)
            bid = market.best_bid(sym)
            if pos <= 0 or not mark or not bid:
                continue
            deal["high"] = max(deal["high"], mark)
            offer = deal["offer"]
            broke = mark < offer * (1 - LISTING_STOP_PCT)
            # The allocation sells on the primary only: a profit-taking exit must
            # clear its fee, and a time exit is not worth a punitive one.
            fee = self.venue_primary.taker
            faded = (deal["high"] >= offer * (1 + LISTING_MIN_GAIN_PCT + 2 * fee)
                     and mark < deal["high"] * (1 - LISTING_TRAIL_PCT))
            stale = now - deal["t0"] >= LISTING_MAX_HOLD_TICKS and fee <= MAX_OPEN_TAKER
            if (broke or faded or stale) and not self.halts.is_halted(sym) and not self._is_pending(sym):
                logger.info("Listing exit %s: sell %d (offer=%.2f high=%.2f mark=%.2f)",
                            sym, pos, offer, deal["high"], mark)
                self._mark_pending(sym)
                return Signal(symbol=sym, side="sell", quantity=min(pos, MAX_ORDER_SHARES),
                              price=round(bid * (1 - SLIP_BPS / 1e4), 2))
        return None

    # ── Hard limits ─────────────────────────────────────────────────────
    def _seat_caps(self, sym: str) -> tuple[int, int]:
        """(most shares long, most shares short) THIS seat may hold in sym.
        All signal seats plus the broker fit inside limit − buffer."""
        if "signal" in self.roles:
            cap = max(0, (self._limit - POSITION_BUFFER - BROKER_MAX_SHARES) // SIGNAL_SEATS)
            return cap, cap
        return (PROBE_SHARES if "probe" in self.roles else 0), 0

    def _mark(self, sym: str, market: MarketData) -> float:
        """Venue mark, else the mid, else (an IPO allocation before it lists)
        the offer price, else what we paid. 0.0 only if nothing is known."""
        return (self._ref.get(sym) or market.mid_price(sym) or self._offer.get(sym)
                or self.risk.book.entry(sym) or 0.0)

    def _equity(self, market: MarketData, portfolio: Portfolio) -> float:
        """Net worth at the venue mark — what the maintenance check uses."""
        # Extra venues fund their own cash at connect: only their P&L counts.
        return (portfolio.cash - getattr(portfolio, "capital_offset", 0.0)
                + sum(q * self._mark(s, market) for s, q in portfolio.positions.items() if q))

    def _risk_off_level(self) -> float:
        capital = self._capital
        if capital is None:
            capital = float(CAPITAL.get(self.team_id, _trader_config.STARTING_CASH))
        return capital * (self._maint + RISK_OFF_MARGIN)

    def _marks(self, market: MarketData, portfolio: Portfolio) -> dict[str, float]:
        """Venue marks for every symbol held or quoted."""
        syms = set(self._ref) | {s for s, q in portfolio.positions.items() if q}
        return {s: m for s in syms if (m := self._mark(s, market))}

    def _close(self, sym: str, market: MarketData, portfolio: Portfolio,
               why: str) -> Signal | None:
        """An IOC that closes (up to MAX_ORDER_SHARES of) the position in sym.
        gate() lets it through every growth limit; halted symbols wait."""
        qty = portfolio.positions.get(sym, 0)
        if not qty or self.halts.is_halted(sym) or self._is_pending(sym):
            return None
        bid, ask = market.best_bid(sym), market.best_ask(sym)
        mark = self._ref.get(sym)                 # after a gap the touch can be stale:
        if qty > 0 and bid:                       # price off the worse of the two
            side, px = "sell", round(min(bid, mark or bid) * (1 - SLIP_BPS / 1e4), 2)
        elif qty < 0 and ask:
            side, px = "buy", round(max(ask, mark or ask) * (1 + SLIP_BPS / 1e4), 2)
        else:
            return None
        self._mark_pending(sym)
        logger.warning("%s: %s %d %s @ %.2f", why, side, abs(qty), sym, px)
        self._end_strategies(sym, why)
        self._closing_sig = Signal(symbol=sym, side=side,
                                   quantity=min(abs(qty), MAX_ORDER_SHARES), price=px)
        return self._closing_sig

    def _end_strategies(self, sym: str, why: str) -> None:
        """A stop overrides the strategy: end whatever was trading sym, or its
        exit logic would buy back what the stop just sold (the signal returns
        to the position it held before the print)."""
        ended = []
        if sym in self.signal.plans:
            self.signal._close(sym, int(self.clock.now()))
            ended.append("signal plan")
        if self._flash.pop(sym, None) is not None:
            ended.append("flash trade")
        if self._listings.pop(sym, None) is not None:
            ended.append("listing")
        if sym in self.leadlag.trades:
            self.leadlag.end(sym, int(self.clock.now()))
            ended.append("lead-lag trade")
        if ended:
            logger.warning("%s %s: ended the %s", why, sym, ", ".join(ended))
            self._save_state()

    def _reduce_largest(self, market: MarketData, portfolio: Portfolio,
                        why: str = "RISK OFF") -> Signal | None:
        """IOC out of the biggest position (near liquidation, or the kill switch)."""
        held = sorted(((abs(q) * self._mark(s, market), s)
                       for s, q in portfolio.positions.items() if q), reverse=True)
        for _, sym in held:
            sig = self._close(sym, market, portfolio, why)
            if sig is not None:
                return sig
        return None

    def gate(self, sig: Signal, market: MarketData, portfolio: Portfolio) -> Signal | None:
        """Shrink or drop an order so it can't break a hard limit. Reducing a
        position is always allowed; only the part that grows it is limited."""
        sym, side, qty, px = sig.symbol, sig.side, sig.quantity, sig.price
        if self._liquidated:
            return self._blocked(sig, "account liquidated")
        if self._stood_down:
            return self._blocked(sig, "another process holds this seat")
        if self._in_auction:
            return self._blocked(sig, "auction: IOCs are refused")
        if self.halts.is_halted(sym):
            return self._blocked(sig, "symbol halted")
        closing = sig is self._closing_sig
        self._closing_sig = None
        marks = self._marks(market, portfolio)
        risk_qty, why = self.risk.check(
            sym, side, qty, px, self.clock.now(), portfolio.positions, marks,
            self._equity(market, portfolio), market.best_bid(sym), market.best_ask(sym),
            closing=closing)
        if risk_qty <= 0:
            return self._blocked(sig, f"risk: {why}")
        if risk_qty < qty:
            logger.info("Risk: %s %s %d -> %d (%s)", side, sym, qty, risk_qty, why)
            qty = risk_qty
        pos = portfolio.positions.get(sym, 0)
        flying = self._in_flight(sym, side)
        max_long, max_short = self._seat_caps(sym)
        team_cap = self._limit - POSITION_BUFFER
        team = pos + self._team_others.get(sym, 0)
        if side == "buy":
            room = min(max_long - pos, team_cap - team) - flying
            shrinks = max(0, -pos) - flying
        else:
            room = min(pos + max_short, team + team_cap) - flying
            shrinks = max(0, pos) - flying
        allowed = shrinks if self._risk_off else max(room, shrinks)
        qty = min(qty, allowed, MAX_ORDER_SHARES)
        if side == "buy" and px > 0:              # the venue checks cash on every buy
            qty = min(qty, int(portfolio.cash * CASH_USE / px))
        if qty <= 0:
            return self._blocked(sig, "risk off: reduce-only" if self._risk_off else
                                 f"limit: pos={pos} in flight={flying} "
                                 f"caps=+{max_long}/-{max_short} team={team}")
        ref = self._ref.get(sym)
        if ref and self._luld > 0:                # the venue rejects prices outside the band
            # Both edges: after a gap the touch can sit outside the band on
            # either side (a stale bid above it after a crash).
            band = max(0.0, self._luld - LULD_MARGIN_PCT)
            lo, hi = -int(-ref * (1 - band) * 100) / 100, int(ref * (1 + band) * 100) / 100
            px = min(max(px, lo), hi)
        if closing:
            logger.info("Gate: closing order %s %s %d passed the risk layer", side, sym, qty)
        if qty != sig.quantity or px != sig.price:
            logger.info("Gate: %s %s %d @ %.2f -> %d @ %.2f", side, sym,
                        sig.quantity, sig.price, qty, px)
        return Signal(symbol=sym, side=side, quantity=qty, price=px)

    def _blocked(self, sig: Signal, why: str) -> None:
        logger.info("Gate blocked %s %d %s: %s", sig.side, sig.quantity, sig.symbol, why)

    def note_sent(self, sig: Signal) -> None:
        """Count an IOC as in flight until its fills land or it dies, and time
        the reaction to a flash on its first order."""
        self._inflight.append([self.clock.now() + PENDING_TICKS, sig.symbol, sig.side, sig.quantity])
        self.msgs_sent += 1
        self.risk.on_send(self.clock.now())
        ft = self._flash.get(sig.symbol)
        if ft is not None and ft.react_ms is None:
            ft.react_ms = (time.monotonic() - ft.wall) * 1000
            row = getattr(ft, "row", {})
            row.update(acted=True, react_ms=ft.react_ms)
            logger.info("FLASH %s reaction: order sent %.0f ms after the event reached us",
                        sig.symbol, ft.react_ms)

    # ── Message quota ───────────────────────────────────────────────────
    def is_urgent(self, sym: str) -> bool:
        """Orders for a live flash may use the venue's burst allowance."""
        return sym in self._flash

    def order_held(self, sym: str) -> None:
        """An order the quota held back: free the symbol to try again at once."""
        self._pending.pop(sym, None)
        ft = self._flash.get(sym)
        if ft is not None and ft.phase == "entry" and ft.tries > 0:
            ft.tries -= 1

    def quota_backoff(self) -> bool:
        return self.clock.now() < self._quota_backoff_until

    def maybe_log_stats(self) -> None:
        """Once a minute: quota use, hot-path timing and the flash scorecard."""
        now = time.monotonic()
        if now - self._stats_at < STATS_EVERY_SEC:
            return
        self._stats_at = now
        sent = self.msgs_sent
        logger.info("STATS quota: %d orders sent, %d RATE_LIMITED (%.1f%%) | hot path: %s | %s | %s | venues: %s",
                    sent, self.rate_limited, 100 * self.rate_limited / max(1, sent + self.rate_limited),
                    self.hot.report(), self.flash_summary(), self.leadlag.summary(),
                    ", ".join(f"{v.name} {v.orders} orders ({v.taker * 1e4:.0f} bps"
                              f"{'' if v.primary or v.connected else ', down'})"
                              for v in [self.venue_primary] + [lk.state for lk in self.venue_links]))
        self.hot.reset()

    def _in_flight(self, sym: str, side: str) -> int:
        """Shares of IOCs sent in sym/side whose fills may not have landed. An
        IOC matches in one pass, so its first fill means the rest dies with it."""
        now = self.clock.now()
        self._inflight = [f for f in self._inflight if now < f[0] and f[3] > 0]
        return sum(f[3] for f in self._inflight if f[1] == sym and f[2] == side)

    def _is_pending(self, sym: str) -> bool:
        return self.clock.now() < self._pending.get(sym, 0.0)

    def _mark_pending(self, sym: str) -> None:
        self._pending[sym] = self.clock.now() + PENDING_TICKS

    # ── Venue callbacks ─────────────────────────────────────────────────
    def on_book(self, symbol: str, ref_price: float) -> None:
        """Keep the venue mark, which the LULD band and maintenance check use."""
        self.risk.on_data(self.clock.now())
        if ref_price > 0 and symbol in self._ll_leaders:
            last = self._ll_last_ref.get(symbol)
            if last and ref_price != last:            # the mark moves once a tick
                self._ll_rets[symbol] = self._ll_rets.get(symbol, 0.0) + math.log(ref_price / last)
            self._ll_last_ref[symbol] = ref_price
        if ref_price > 0:
            self._ref[symbol] = ref_price
            fair = self._fair.get(symbol)
            if fair is None:
                self._fair[symbol] = ref_price
            elif abs(ref_price - fair) > FAIR_JUMP_PCT * fair:
                # A flash: keep the pre-flash fair, unless the move sticks.
                n = self._fair_off.get(symbol, 0) + 1
                self._fair_off[symbol] = n
                if n >= FAIR_RESET_SNAPSHOTS:
                    self._fair[symbol], self._fair_off[symbol] = ref_price, 0
            else:
                self._fair_off[symbol] = 0
                self._fair[symbol] = fair + FAIR_ALPHA * (ref_price - fair)

    def on_open(self, data: dict) -> None:
        """SESSION_OPEN: adopt the week's rules from the scenario it carries."""
        sc = data.get("scenario") or {}
        cfg = sc.get("config") or {}
        self.clock.note(data.get("tick"))
        self._in_auction = False
        self._shorts_ok = bool((sc.get("flags") or {}).get("shorts_allowed", False))
        self._limit = int(sc.get("position_limit") or self._limit)
        self._maint = float(sc.get("maintenance_fraction") or self._maint)
        self._ssr_pct = float(cfg.get("SSR_TRIGGER_PCT", self._ssr_pct))
        self._locate_cap = int(cfg.get("SHORT_LOCATE_CAP", self._locate_cap))
        self._luld = float(cfg.get("LULD_BAND_PCT", self._luld))
        self.venue_primary.luld = self._luld
        if data.get("late_join"):
            self._load_state()
            # A restart mid-block: make sure the primary still sees an order.
            if "probe" in self.roles and self.venue_primary.orders == 0 and not self._probe_queue:
                self._probe_queue = [PROBE_SYMBOL]
        else:                                     # all sticky until the next open
            self._event_t0 = self.clock.now()
            self.leadlag.new_session()
            self._state_loaded = True
            self.signal.plans.clear()
            self._listings.clear()
            self._offer.clear()
            self._flash.clear()
            self._save_state()
            self._ssr.clear()
            self._open_mid.clear()
            self.halts.clear()
            self.risk.new_session(self.clock.now())
            self._liquidated = False
            self._capital = None
            if "probe" in self.roles:
                self._probe_queue = [PROBE_SYMBOL]
                self._probe = None
        logger.info("Session rules: shorts=%s limit=%d SSR=%.1f%% LULD=%.1f%% maint=%.0f%% | "
                    "seat caps +%d/-%d, fee %.0f bps", self._shorts_ok, self._limit,
                    self._ssr_pct * 100, self._luld * 100, self._maint * 100,
                    *self._seat_caps(""), self.signal.taker_fee * 1e4)

    def on_leaderboard(self, msg: Leaderboard) -> None:
        """Clock, borrow, team positions, capital, and the duplicate-seat check."""
        self.clock.note(msg.tick)
        self._short_interest = short_interest(msg)
        self._team_others = team_positions(msg, self.team_id)
        me = next((e for e in msg.traders if e.get("team_id") == self.team_id), None)
        if me is None:
            return
        if self._capital is None and "net_worth" in me:
            self._capital = capital_baseline(float(me["net_worth"]), CAPITAL.get(self.team_id))
        if self.portfolio is not None and "positions" in me:
            venue = {s: int(q) for s, q in me["positions"].items() if q}
            primary = getattr(self.portfolio, "primary", self.portfolio)   # this board is the primary's
            mine = {s: int(q) for s, q in primary.positions.items() if q}
            self._view_mismatch = self._view_mismatch + 1 if venue != mine else 0
            if self._view_mismatch >= STAND_DOWN_BOARDS and not self._stood_down:
                self._stood_down = True
                logger.error("STANDING DOWN: the venue says %s holds %s, this process thinks %s. "
                             "Another process has this seat's connection — stop the extra one.",
                             self.team_id, venue, mine)

    # ── Restart safety ──────────────────────────────────────────────────
    def _save_state(self) -> None:
        """Keep open trades on disk. A restarted process would otherwise hold
        shares it has no plan to exit."""
        if self._state_path is None:
            return
        state = {"tick": self.clock.now(), "listings": self._listings,
                 "plans": {s: asdict(p) for s, p in self.signal.plans.items()}}
        tmp = self._state_path.with_suffix(".tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(state))
        tmp.replace(self._state_path)

    def _load_state(self) -> None:
        """Once per process, on joining a session already under way."""
        if self._state_loaded or self._state_path is None or not self._state_path.exists():
            return
        self._state_loaded = True
        try:
            state = json.loads(self._state_path.read_text())
            if self.clock.now() - state["tick"] > STATE_MAX_AGE_TICKS:
                return
            self.signal.plans.update({s: Plan(**p) for s, p in state["plans"].items()})
            self._listings.update(state["listings"])
        except (OSError, ValueError, KeyError, TypeError):
            logger.exception("Ignoring an unreadable state file")
            return
        logger.warning("Restored after a restart: plans %s, listings %s",
                       list(state["plans"]), list(state["listings"]))

    def on_reject(self, code: str | None, message: str | None) -> None:
        """React to a venue reject: shorts, halts, liquidation, IPO cash."""
        logger.warning("REJECT [%s]: %s", code, message)
        self.risk.on_reject(code, self.clock.now())
        if code == "RATE_LIMITED":                # the team's quota is spent: ease off
            self.rate_limited += 1
            self.msgs_sent = max(0, self.msgs_sent - 1)   # it never reached the book
            self._quota_backoff_until = self.clock.now() + QUOTA_BACKOFF_TICKS
        elif code in SHORT_REJECTS:                 # a rejected short: no new ones for a while
            self._no_short_until = self.clock.now() + 30
        elif code == "SYMBOL_HALTED":
            self.halts.on_reject(message)
        elif code == "LIQUIDATED":
            self._liquidated = True
        elif code == "IPO_REJECTED":              # usually cash: retry smaller
            for ind in self._ipo.values():
                ind["qty"] = max(1, ind["qty"] // 2)
                ind["next"] = self.clock.now()

    def on_fill(self, side: str, symbol: str, quantity: int, price: float) -> None:
        """Settle the in-flight count and charge the fill to the plan or the probe."""
        now = self.clock.now()
        self.risk.on_fill(symbol, side, quantity, price)
        if self._is_pending(symbol):              # an IOC fills at once; let the rest land
            self._pending[symbol] = min(self._pending[symbol], now + FILL_SETTLE_TICKS)
        left = quantity                           # these shares are no longer in flight
        for f in self._inflight:
            if f[1] == symbol and f[2] == side and left > 0:
                take = min(left, f[3])
                f[3] -= take
                f[0] = min(f[0], now + FILL_SETTLE_TICKS)
                left -= take
        ft = self._flash.get(symbol)
        if symbol in self.signal.plans:           # a plan's base includes any probe shares
            self.signal.on_fill(symbol, side, quantity, price)
            self._save_state()
        elif self.leadlag.on_fill(symbol, side, quantity, price):
            logger.info("LEADLAG fill: %s %d %s @ %.2f", side, quantity, symbol, price)
        elif ft is not None:
            ft.on_fill(side, quantity, price)
            row = getattr(ft, "row", {})
            if side == ft.side:
                row["filled"] = row.get("filled", 0) + quantity
            logger.info("FLASH %s fill: %s %d @ %.2f (fair %.2f, now holding %+d)",
                        symbol, side, quantity, price, ft.fair, ft.held)
        elif self._probe is not None and self._probe.symbol == symbol and self._probe.phase != "done":
            self._probe.on_fill(side, quantity, now)

    def on_event(self, event: str, message: str, data: dict) -> None:
        """Session events: halts, prints, fees, SSR, auctions, IPO, liquidation."""
        logger.info("Event: %s | %s | %s", event, message, data)
        self.clock.note(data.get("tick"))
        for sym in self.halts.on_event(event, data):
            self._on_resumed(sym)
        if event == "FLASH":
            self._on_flash(data)
        elif event == "WEEK_CHANGED":            # the graded block starts: count it alone
            self.leadlag.new_session()
            self._stats_at = 0.0
            self.maybe_log_stats()
            self.msgs_sent = self.rate_limited = 0
            self.flash_log = []
            self.risk.new_session(self.clock.now())   # open-play losses don't count
            logger.info("WEEK → %s: quota and flash counters reset", data.get("week"))
        elif event == "FEE_SCHEDULE" and isinstance(data.get("taker"), (int, float)):
            self.venue_primary.on_fee_schedule(data)
            self.signal.set_fee(self.route_fee())
        elif event == "CALENDAR":
            if self._event_t0 is not None:        # calendar ticks count from the opening cross
                self.leadlag.on_calendar([{**e, "tick": e["tick"] + self._event_t0}
                                          for e in (data.get("events") or []) + (data.get("imminent") or [])
                                          if isinstance(e.get("tick"), (int, float))])
        elif event == "CALENDAR_EVENT" and isinstance(data.get("pct"), (int, float)):
            self.leadlag.on_print(int(data.get("tick") or self.clock.now()), data.get("affected"),
                                  bool(data.get("market_wide")))
            for sym in data.get("affected") or []:
                self._on_print(sym, float(data["pct"]))
        elif event == "SHOCK":                    # teacher-injected: target price per symbol
            for sym, target in (data.get("prices") or {}).items():
                mark = self._ref.get(sym)
                if mark and isinstance(target, (int, float)) and abs(target / mark - 1) >= MIN_SHOCK_PCT:
                    self._on_print(sym, target / mark - 1)
        elif event == "SSR_ON" and data.get("symbol"):
            self._ssr.add(data["symbol"])
        elif event in ("SESSION_PREOPEN", "SESSION_PRECLOSE"):
            self._in_auction = True
        elif event == "IPO_SUB_RESULT" and data.get("ok"):
            self._ipo.pop(data.get("symbol"), None)
            logger.info("IPO indication confirmed: %s", message)
        elif event == "IPO_PRICED":
            self._ipo.pop(data.get("symbol"), None)
            if data.get("symbol") and isinstance(data.get("offer_price"), (int, float)):
                # The allocation is paid for now but has no mark until it lists:
                # value it at the offer, or the session stop sees a fake loss.
                self._offer[data["symbol"]] = float(data["offer_price"])
        elif event == "IPO_LISTED" and data.get("symbol") and data.get("offer_price"):
            offer = float(data["offer_price"])
            self._listings[data["symbol"]] = {"offer": offer, "high": offer, "t0": self.clock.now()}
            self._save_state()
        elif event == "LIQUIDATION" and data.get("team_id") == self.team_id:
            self._liquidated = True
            logger.error("This seat was LIQUIDATED — no further orders this session")

    def on_venue_fill(self, side: str, symbol: str, quantity: int, price: float, venue: str) -> None:
        """A fill on an extra venue: the strategies account for it like any other."""
        logger.info("Fill on %s: %s %d %s @ %.2f", venue, side, quantity, symbol, price)
        self.on_fill(side, symbol, quantity, price)

    def on_venue_reject(self, code: str | None, message: str | None, venue: str) -> None:
        """A reject on an extra venue. Its halts are its own (the link tracks
        them); everything else counts toward the reject-storm monitor."""
        if code == "SYMBOL_HALTED":
            logger.info("REJECT on %s [%s]: %s — that venue only", venue, code, message)
            return
        self.on_reject(code, f"[{venue}] {message}")

    def run(self) -> None:
        """Connect and trade until interrupted."""
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s  %(levelname)-8s  %(message)s",
                            datefmt="%H:%M:%S")
        self.venue_spec = os.environ.get("TRADER_VENUES", "auto")
        asyncio.run(TakerBot(self).run())


class _WireTap:
    """Wraps the websocket so the bot sees OrderAck and ErrorMsg, which the
    stock bot only logs."""

    def __init__(self, ws: Any, bot: TakerBot) -> None:
        self._ws, self._bot = ws, bot

    def __getattr__(self, name: str) -> Any:
        return getattr(self._ws, name)

    def __aiter__(self) -> AsyncIterator[str]:
        return self._tap()

    async def _tap(self) -> AsyncIterator[str]:
        hot = self._bot._owner.hot
        clock = time.perf_counter
        async for raw in self._ws:
            t0 = clock()
            if '"order_ack"' in raw:
                try:
                    self._bot.on_ack(raw)
                except Exception:
                    logger.exception("ignoring unreadable order_ack")
            elif '"error"' in raw:
                try:
                    msg = json.loads(raw)
                    if msg.get("type") == "error":
                        self._bot._owner.on_reject(msg.get("code"), msg.get("message"))
                except Exception:
                    logger.exception("ignoring unreadable error")
            yield raw                             # listen() handles it before we resume
            hot.record(clock() - t0)


class _CombinedStrategy:
    """The stock loop calls strategy.generate_signal(market, portfolio) with
    the primary's portfolio; the trader must see every venue's positions."""

    def __init__(self, owner: MyTrader) -> None:
        self._owner = owner

    def generate_signal(self, market: MarketData, portfolio: Portfolio) -> Signal | None:
        return self._owner.on_tick(market, self._owner.portfolio)


class TakerBot(_AdaptedTraderBot):
    """The standard bot (on_tick → order), with every Signal sent as an IOC,
    the hard limits in front of the wire, and portfolio recovery on reconnect."""

    def __init__(self, owner: MyTrader) -> None:
        super().__init__(owner)
        self._in_session = False
        self._portfolio_synced = False
        self._sent_at: deque[float] = deque()     # monotonic send times, last ~1 s
        self._venue_sent: dict[str, deque[float]] = {}
        self._acting = False
        self.links = self._make_links(owner.venue_spec)
        owner.venue_links = self.links
        # The strategies and the risk layer see every venue's positions as one.
        owner.portfolio = CombinedPortfolio(self.portfolio, [lk.state for lk in self.links])
        self.strategy = _CombinedStrategy(owner)

    def _make_links(self, spec: str) -> list[VenueLink]:
        """Extra venues: "none", "auto" (the arena's venue list), or a comma
        list of URLs / ports."""
        spec = (spec or "none").strip()
        if spec.lower() in ("", "none", "0", "off"):
            return []
        primary = _trader_config.EXCHANGE_URL
        if spec.lower() == "auto":
            found = discover_venues(primary)[:MAX_SECONDARY]
            venues = [(v["name"], v["url"]) for v in found]
        else:
            from team.venues import venue_url
            venues = []
            for item in (x.strip() for x in spec.split(",") if x.strip()):
                url = venue_url(primary, int(item)) if item.isdigit() else item
                venues.append((item, url))
        owner = self._owner
        links = [VenueLink(name, url, owner.team_id, _trader_config.ARENA_TOKEN,
                           owner.on_venue_fill, owner.on_venue_reject) for name, url in venues]
        logger.info("Venues: primary %s + %s", primary,
                    ", ".join(f"{lk.state.name} ({lk.url})" for lk in links) or "no others")
        return links

    async def run(self) -> None:
        """The primary connection (with its reconnect loop) and every extra venue."""
        if not self.links:
            await super().run()
            return
        await asyncio.gather(super().run(), *(lk.run() for lk in self.links))

    async def connect(self) -> None:
        """Connect, tap the socket, and track whether the portfolio is synced."""
        await super().connect()
        self._ws = _WireTap(self._ws, self)
        self._owner.risk.on_connect(self._owner.clock.now())
        # A reconnect gets no PortfolioUpdate until the first fill, so cash and
        # positions come from the leaderboard (every ~2 s) until one arrives.
        self._portfolio_synced = False
        apply_update = type(self.portfolio).apply_server_update

        def synced_apply(update: PortfolioUpdate, _portfolio: Portfolio = self.portfolio) -> None:
            apply_update(_portfolio, update)
            self._portfolio_synced = True
        self.portfolio.apply_server_update = synced_apply

    @property
    def market(self) -> MarketData:
        """The book view, rebuilt by run() on every reconnect."""
        return self._market

    @market.setter
    def market(self, value: MarketData) -> None:
        # run() builds a fresh MarketData on every reconnect, so the mark feed
        # has to be attached to each one rather than wrapped once at startup.
        update_book = value.update_book

        def update_book_and_mark(snapshot: BookSnapshot) -> None:
            update_book(snapshot)
            owner = self._owner
            owner.venue_primary.on_book(snapshot.symbol, snapshot.bids, snapshot.asks, snapshot.ref_price)
            owner.on_book(snapshot.symbol, snapshot.ref_price)
            # A fresh book in a symbol we are fading: decide now, not at the
            # next 0.5 s tick (the flash is over in ~2 s).
            if snapshot.symbol in owner._flash and not self._acting and self._session_open:
                self._acting = True
                asyncio.get_running_loop().create_task(self._act_now())
        value.update_book = update_book_and_mark
        # The stock MarketData re-slices a 500-trade list on every trade
        # message (O(n) per message); a bounded deque appends in O(1).
        value._recent_trades = deque(maxlen=500)
        value.update_trade = value._recent_trades.append
        self._market = value

    def on_ack(self, raw: str) -> None:
        """Log our own order acknowledgements."""
        ack = OrderAck.model_validate_json(raw)
        if ack.team_id == self._owner.team_id:
            logger.info("order_ack -> id: %s %s %d %s @ %.2f",
                        ack.order_id, ack.side, ack.quantity, ack.symbol, ack.price)

    async def _on_session_event(self, msg: SessionEvent) -> None:
        owner = self._owner
        if msg.event in ("SESSION_PREOPEN", "SESSION_OPEN"):
            self._in_session = True
        elif msg.event == "SESSION_CLOSED":
            self._in_session = False
        await super()._on_session_event(msg)      # runs MyTrader.on_event
        if msg.event == "SESSION_OPEN":
            owner.on_open(msg.data or {})
        # React to a print now instead of at the next tick: stale quotes go to
        # whoever is fastest.
        if msg.event in ("CALENDAR_EVENT", "SHOCK", "FLASH") and self._session_open:
            await self._act_now()

    async def _act_now(self) -> None:
        try:
            signal = self._owner.on_tick(self.market, self._owner.portfolio)
            if signal is not None:
                await self._place_order(signal)
        except Exception:
            logger.exception("on_tick raised on an event — waiting for the next tick")
        finally:
            self._acting = False

    def _on_leaderboard(self, msg: Leaderboard) -> None:
        if self._in_session and not self._session_open:
            self._session_open = True         # reconnected mid-session: reopen the gate
            logger.info("Session still live after reconnect — trading gate reopened")
        super()._on_leaderboard(msg)
        self._owner.on_leaderboard(msg)
        self._owner.maybe_log_stats()
        if self._portfolio_synced:
            return
        for entry in msg.traders:
            if entry.get("team_id") == self._owner.team_id:
                self.portfolio.cash = float(entry.get("cash", self.portfolio.cash))
                self.portfolio.positions = {k: int(v) for k, v in
                                            (entry.get("positions") or {}).items()}
                self._portfolio_synced = True
                logger.info("Portfolio seeded from leaderboard: cash=%.2f positions=%s",
                            self.portfolio.cash, self.portfolio.positions)
                break

    async def flatten_all_positions(self) -> None:
        """Hold through the close (see below)."""
        # The stock bot flattens on SESSION_CLOSED, after the venue has closed,
        # so every order would be rejected. Hold through the close instead.
        logger.info("Session closed — holding positions (no flatten)")

    async def trading_loop(self) -> None:
        """The standard loop, plus the IPO indication watcher beside it."""
        watcher = asyncio.create_task(self._ipo_watch())
        try:
            await super().trading_loop()
        finally:
            watcher.cancel()

    async def _ipo_watch(self) -> None:
        """Resend an IPO indication until the venue confirms it."""
        while True:
            await asyncio.sleep(1.0)
            request = self._owner.pending_ipo()
            if request is not None:
                try:
                    await self._send(request)
                except Exception:
                    logger.exception("IPO resend failed — will retry")

    async def _place_order(self, signal: Signal) -> None:
        # Every order, from the tick loop or an event, passes the hard limits
        # here. A send that raises is dropped, not propagated: trading_loop
        # would die while the listener kept the bot "connected" but idle. If
        # the socket is gone, listen() ends too and run() reconnects both.
        owner = self._owner
        urgent = owner.is_urgent(signal.symbol)
        pinned = owner.venue_for(signal)
        now = time.monotonic()
        gated = owner.gate(signal, self.market, owner.portfolio)
        if gated is None:
            return
        r = self._route(gated, pinned)
        if r is None:
            logger.info("Route: no venue can take %s %d %s @ %.2f — dropped",
                        gated.side, gated.quantity, gated.symbol, gated.price)
            owner.order_held(gated.symbol)
            return
        # Quotas are per venue: the primary's is the team's (shared with the
        # broker), each extra venue meters its own.
        sent = self._sent_at if r.venue == "primary" else self._venue_sent.setdefault(r.venue, deque())
        while sent and now - sent[0] > 1.05:
            sent.popleft()
        cap = (TRADER_FLASH_BURST if urgent else TRADER_MSGS_PER_SEC) if r.venue == "primary" else VENUE_MSGS_PER_SEC
        if len(sent) >= cap or (r.venue == "primary" and owner.quota_backoff() and not urgent):
            logger.info("Quota: holding %s %d %s for %s (%d sent in the last second)",
                        gated.side, gated.quantity, gated.symbol, r.venue, len(sent))
            owner.order_held(gated.symbol)
            return
        out = Signal(symbol=gated.symbol, side=gated.side, quantity=r.quantity, price=r.price)
        try:
            if r.venue == "primary":
                await self._send(PlaceOrder(team_id=owner.team_id, symbol=out.symbol, side=out.side,
                                            order_type="ioc", price=out.price, quantity=out.quantity))
                owner.venue_primary.orders += 1
            else:
                link = next(lk for lk in self.links if lk.state.name == r.venue)
                if not await link.send_order(out.symbol, out.side, out.quantity, out.price):
                    owner.order_held(out.symbol)
                    return
        except Exception:
            logger.exception("Order send failed: %s %d %s — dropped", out.side, out.quantity, out.symbol)
            return
        sent.append(now)
        owner.note_sent(out)
        logger.info("Order: IOC %s %d %s @ %.2f on %s (%s)",
                    out.side, out.quantity, out.symbol, out.price, r.venue, r.why)

    def _route(self, sig: Signal, pinned: str | None):
        """Pick the venue for a gated order (team/venues.py route())."""
        owner = self._owner
        prim = owner.venue_primary
        prim.connected = self._ws is not None
        prim.session_open = self._session_open
        prim.in_auction = owner._in_auction
        prim.cash = self.portfolio.cash
        prim.positions = dict(self.portfolio.positions)
        prim.halted = {s for s in prim.quotes if owner.halts.is_halted(s)}
        direct = Route("primary", sig.quantity, sig.price, sig.price, "primary")
        if not self.links or pinned == "primary":
            return direct                         # single venue: exactly as before
        states = [prim] + [lk.state for lk in self.links]
        # Opening exposure only where the fee is payable; a close may go
        # anywhere that holds the position (route() keeps it there).
        total = sum(v.positions.get(sig.symbol, 0) for v in states)
        opening = total == 0 or (total > 0) == (sig.side == "buy")
        if opening:
            states = [v for v in states if v.taker <= MAX_OPEN_TAKER] or states
        r = route(sig.symbol, sig.side, sig.quantity, sig.price, states)
        if r is None:
            # No venue shows a fill at the limit. The primary IOC may still find
            # one — if the primary may take this order: an open at a payable fee,
            # or a close of a position held THERE (never a cross-venue short).
            held = prim.positions.get(sig.symbol, 0) * (1 if sig.side == "buy" else -1)
            if (opening and prim.taker <= MAX_OPEN_TAKER) or (not opening and held < 0):
                return Route("primary", min(sig.quantity, abs(held)) if not opening else sig.quantity,
                             sig.price, sig.price, "primary (no venue shows a fill)")
        return r


if __name__ == "__main__":
    _lock = single_instance(_trader_config.TEAM_ID)
    MyTrader(state_path=STATE_DIR / f"{_trader_config.TEAM_ID}.json").run()
