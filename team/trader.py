"""
STT_11 — trader bot (market taking).

Implement on_tick(): each tick, return at most one Signal to trade, or None
to sit out. Orders go out as IOC (immediate-or-cancel): they fill against the
book at your price or better and any remainder is cancelled, so nothing is
left resting. To take liquidity, price a buy at or above best_ask and a sell
at or below best_bid. Market making lives in team/broker.py.

Run one seat:
    TEAM_ID=stt_11_trader_1 python -m team.trader
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time

import trader.config as _trader_config
from shared.messages import OrderAck, PlaceOrder, Signal

from arena import Trader
from arena.trader import _AdaptedTraderBot
from team.config import CAPITAL

logger = logging.getLogger(__name__)

# ── Strategy settings ────────────────────────────────────────────────────
# TODO: add your strategy parameters here.
MIN_EDGE_BPS = 130        # enter only if this much move is left (2 taker fees + spread)
MR_WINDOW = 30            # ticks of mid history
MR_Z = 2.0                # enter when mid is this many std devs below the mean
MR_MIN_EDGE_BPS = MIN_EDGE_BPS  # reversion to the mean must beat 2 taker fees + spread
MR_SHARES = 50
MR_STOP_BPS = 150         # cut the trade if bid falls this far below entry
MR_MAX_HOLD_SEC = 60      # or if it hasn't reverted in this long
SKIP_SYMBOLS = ("ARENA10",) #not supposed to trade futures 

# ── Earnings-shock settings ──────────────────────────────────────────────
# On CALENDAR_EVENT the exchange publishes the signed move (pct), then ramps
# fair value to overshoot × pct over ramp_ticks and settles back to 1.0 × pct
# over about half as many again (docs/SEASON_GUIDE.md §6).
EXCHANGE_TICK_SEC = 1.0   # the exchange advances prices once per second
SHOCK_SHARES = 550        # most shares to hold in a shock symbol
POSITION_LIMIT = 800      # exchange per-symbol cap (week 4: 800) — over it is rejected;
                          # replaced at SESSION_OPEN by the scenario's position_limit
POSITION_BUFFER = 20      # stay this far under POSITION_LIMIT
# The limit is per TEAM and counts every seat. team/broker.py holds at most
# BROKER_MAX_SHARES long (position + resting bids) and BROKER_MAX_SHORT short
# (position − resting asks) in any symbol it quotes, and never quotes
# BROKER_SKIP_SYMBOLS — keep all three in sync with team/broker.py. The live
# block runs ONE broker seat; with two, double both BROKER_ numbers.
BROKER_MAX_SHARES = 150
BROKER_MAX_SHORT = 150
BROKER_SKIP_SYMBOLS = ()
SLIP_BPS = 20             # price IOCs this far through the touch so they fill
# Shock IOCs reach further: the ramp moves |pct| x overshoot / ramp_ticks per
# tick (26-78 bps for these prints), so 20 bps only reaches the top level or
# two. Shock slip = this fraction of one tick's move, clamped to [SLIP_BPS, max].
SHOCK_SLIP_TICK_FRACTION = 0.75
SHOCK_SLIP_MAX_BPS = 60
MAX_ORDER_SHARES = 500    # exchange rejects any single order above this (INVALID_QUANTITY)

EXIT_FRACTION = 0.9       # sell once price covers this much of the move to the peak
FADE_NEGATIVE = True      # after bad news, buy the overshoot and sell the recovery

# ── Shorting the shock (week 4+: shorts unlock) ──────────────────────────
# Bad news: short on the first ticks of the drop, cover near the overshoot low.
# Good news: after the long exits near the peak, short the overshoot and cover
# as it settles back to 1.0 × pct. The broker never shorts and stops quoting a
# symbol once its print is announced, so the team cap is the trader's to use.
SHORT_SHARES = 400        # most shares to be short in a shock symbol (borrow is
                          # capped at 2,000/symbol across the whole market)
FADE_POSITIVE = True      # after good news, short the overshoot peak
# Short-sale rule: once a symbol is SSR_TRIGGER_PCT below the session open, a
# short that hits the bid is rejected (SSR_RESTRICTED). Stop opening shorts
# this far before the trigger; covering is always allowed.
SSR_TRIGGER_PCT = 0.06    # week 4 value; replaced at SESSION_OPEN from the scenario
SSR_MARGIN_PCT = 0.01
SHORT_REJECTS = ("SHORTS_LOCKED", "SSR_RESTRICTED", "BORROW_UNAVAILABLE")
# Borrow is capped market-wide (SHORT_LOCATE_CAP, from the scenario). Only
# open a short while short interest, read from the leaderboard every ~2 s,
# leaves room for it plus this much from shorts placed since.
SHORT_LOCATE_CAP = 2000
BORROW_HEADROOM = 300

# ── Seat roles ───────────────────────────────────────────────────────────
# Every trader seat runs this file; TEAM_ID picks what it does. Seats split
# the team's per-symbol cap instead of racing each other for it:
#   shock    earnings / teacher-shock play     (up to SHOCK_SHARES long,
#                                               SHORT_SHARES short)
#   starter  one guaranteed buy-then-sell after the open
#   mr       mean reversion, MR_SHARES long per symbol
#   ipo      bid in IPO bookbuilds
# A seat not listed here trades nothing. TRADER_ROLES="shock,mr" overrides
# (for local testing).
SEAT_ROLES = {
    "stt_11_trader_1": {"shock", "starter", "ipo"},
    "stt_11_trader_2": {"mr", "ipo"},
}
# Longs other seats may hold in a shock symbol on top of the broker: trader_2's
# one mean-reversion position. Keeps the team under the cap on good news.
OTHER_SEAT_SHARES = MR_SHARES
TOKEN_SHARES = 5          # bad news, nothing to dump, too small to fade: still buy this
                          # many at the low, so every shock gets an order (graded)
MIN_SHOCK_PCT = 0.005     # ignore teacher SHOCK moves smaller than this
ORDER_GAP_SEC = 0.4       # wait this long after an order so its fill can arrive
PENDING_TIMEOUT_SEC = 1.0 # no new order in a symbol until its last one settles (or this long)
FILL_SETTLE_SEC = 0.15    # after a fill, wait this long for the rest of the IOC's fills

# ── Starter order (Level 1: guarantee one fill + ack) ────────────────────
# Once per run: buy a few shares as soon as the book is two-sided, hold
# briefly, then sell them back. Not AAPL, so it never mixes with the shock.
STARTER_SYMBOL = "MSFT"
STARTER_QTY = 5           # small and size-limited
STARTER_HOLD_SEC = 10     # hold this long after the buy fills, then sell

# ── Event / IPO settings ─────────────────────────────────────────────────
ON_EVENT_TIME = 120   # seconds to pause a symbol after an event names it
IPO_SHARES = 40       # most IPO shares to bid for

# ── Week 5 risk: halts, LULD band, liquidation ───────────────────────────
# Every order passes _gate() (called from TakerBot._place_order, the one
# place orders leave the bot), which shrinks or drops it so that:
#   - this seat stays inside _seat_caps() in every symbol, counting IOCs
#     still in flight, and the TEAM (other seats from the leaderboard) stays
#     inside position_limit − POSITION_BUFFER
#   - gross notional stays ≤ MAX_GROSS_LEVERAGE × net worth, so even a 30%
#     move against the whole book can't reach the liquidation line
#   - below RISK_OFF_MARGIN above the maintenance line only orders that
#     shrink a position go out, and _risk_off_signal() flattens the largest
#   - nothing is sent into a halted symbol (the venue would reject it)
#   - limit prices sit inside the LULD band around the venue mark
# A halt's ramp keeps running while the symbol is frozen: the shock play
# holds through it and unwinds on TRADING_RESUMED. After every resume the
# starter seat buys and sells STARTER_QTY in that symbol, so the bot
# visibly trades it again.
MAINTENANCE_FRACTION = 0.5  # week 5; replaced at SESSION_OPEN from the scenario
RISK_OFF_MARGIN = 0.20      # go reduce-only this far above the maintenance line
MAX_GROSS_LEVERAGE = 1.5
LULD_BAND_PCT = 0.08        # week 5; replaced at SESSION_OPEN from the scenario
LULD_MARGIN_PCT = 0.005     # keep limit prices this far inside the band
HALT_FALLBACK_SEC = 300     # halt length to assume if the event didn't say
HALT_SLACK_SEC = 5          # past resume_at, assume we missed TRADING_RESUMED
RESUME_PROBE_DELAY_SEC = 3  # let the book refill before the probe buy
INFLIGHT_SEC = 2.0          # an IOC's fills land well inside this
OWN_TEAM_PREFIX = "stt_11_"



class MyTrader(Trader):
    """Your alpha lives here."""

    def __init__(self):
        super().__init__()
        self._mr: dict[str, dict] = {} #{held, entry, t} for every mean reversion
        self._preopen = False
        self._bought = {}                         # symbol -> (shares, avg price)
        self._paused = {} #keep track of what stocks are being paused atm
        self._last_mid = {}                       # symbol -> mid at the last tick
        self._shocks: dict[str, dict] = {}        # symbol -> earnings shock being traded
        self._pending: dict[str, float] = {}      # symbol -> time its in-flight order settles
        self._ramp = (15, 1.3)                    # (ramp_ticks, overshoot), updated from CALENDAR_EVENT
        self._starter = new_starter(STARTER_SYMBOL)
        self._probes: list[str] = []              # symbols to probe once the starter is free
        self._books: dict[str, list] = {}   # symbol -> [position, avg_entry, realized]
        self._mismatch: set[str] = set()
        self._ipo_px: dict[str, float] = {}
        roles = os.environ.get("TRADER_ROLES")
        self._roles = (set(r.strip() for r in roles.split(",") if r.strip()) if roles
                       else SEAT_ROLES.get(_trader_config.TEAM_ID, set()))
        logger.info("Seat %s roles: %s", _trader_config.TEAM_ID,
                    ", ".join(sorted(self._roles)) or "NONE — this seat will not trade")
        self._shorts_ok = False                   # set from the scenario at SESSION_OPEN
        self._limit = POSITION_LIMIT
        self._ssr_pct = SSR_TRIGGER_PCT
        self._locate_cap = SHORT_LOCATE_CAP
        self._short_interest: dict[str, int] = {} # symbol -> market-wide shares short
        self._ssr: set[str] = set()               # symbols under the short-sale rule
        self._open_mid: dict[str, float] = {}     # symbol -> first mid after the open
        self._cash = 0.0
        self._last_recon = 0.0
        self._last_status = 0.0
        # Risk state for _gate()
        self._maint = MAINTENANCE_FRACTION
        self._luld = LULD_BAND_PCT
        self._start_equity: float | None = None   # set from the first leaderboard
        self._ref: dict[str, float] = {}          # symbol -> venue mark (BookSnapshot.ref_price)
        self._halted: dict[str, float] = {}       # symbol -> wall-clock time it should resume
        self._market_halt_until = 0.0             # wall clock; 0 = no market-wide halt
        self._inflight: list[list] = []           # [sent_at, symbol, side, unfilled qty]
        self._team_others: dict[str, int] = {}    # symbol -> other team seats' position
        self._liquidated = False
        self._risk_off = False

    def on_tick(self, market, portfolio) -> Signal | None:
        """Each tick: refresh state, then shock play > starter > mean reversion."""
        #pre-shock reference mids
        for sym in market.symbols():
            mid = market.mid_price(sym)
            if mid:
                self._last_mid[sym] = mid

        #bookkeeping checks
        now = time.monotonic()
        self._cash = portfolio.cash
        if now - self._last_recon > 5:
            self._last_recon = now
            self._reconcile(market, portfolio)
        if now - self._last_status > 60:
            self._last_status = now
            self._log_status(market, portfolio)

        #venue rejects IOCs until the opening cross
        if self._preopen or self._liquidated:
            return None
        for sym, mid in self._last_mid.items():   # SSR is measured from the open
            self._open_mid.setdefault(sym, mid)

        #near the liquidation line: only shrink the book
        self._risk_off = self._equity(market, portfolio) < self._risk_off_level()
        if self._risk_off:
            return self._risk_off_signal(market, portfolio)

        #an earnings shock gets first call on every tick (halted ones wait)
        for sym in [s for s in self._shocks if not self._is_halted(s)]:
            signal = self._trade_shock(self._shocks[sym], market, portfolio)
            if signal is not None:
                return signal

        #starter order (guaranteed fill + ack), then one probe per resumed
        #halt — never in a symbol a shock is still trading (its fills would
        #land in the shock's share count)
        if "starter" in self._roles:
            if self._starter["phase"] == "done":
                ready = [s for s in self._probes if s not in self._shocks]
                if ready:
                    self._probes.remove(ready[0])
                    self._starter = new_starter(ready[0], now + RESUME_PROBE_DELAY_SEC)
                    logger.info("Resume probe: round trip of %d %s", STARTER_QTY, ready[0])
            if self._starter["phase"] != "done" and self._starter["symbol"] not in self._shocks:
                return self._trade_starter(market, portfolio)

        #mean reversion
        if "mr" not in self._roles:
            return None
        for sym in market.symbols():
            if (sym in SKIP_SYMBOLS or self._is_pending(sym) or self._is_halted(sym)
                    or now < self._paused.get(sym, 0)):
                continue
            bid, ask = market.best_bid(sym), market.best_ask(sym)
            mid = market.mid_price(sym)
            hist = market.prices(sym)
            if bid is None or ask is None or not mid or len(hist) < MR_WINDOW:
                continue                     # also the re-sync guard after a reconnect

            w = hist[-MR_WINDOW:]
            mean = sum(w) / MR_WINDOW
            std = (sum((x - mean) ** 2 for x in w) / (MR_WINDOW - 1)) ** 0.5
            if std == 0:
                continue
            z = (mid - mean) / std           # how many std devs the price is from its average

            pos = self._mr.get(sym)
            if pos is not None:              # exit an open MR trade
                if pos["held"] <= 0:         # entry IOC missed entirely
                    self._mr.pop(sym)
                    continue
                stopped = bid < pos["entry"] * (1 - MR_STOP_BPS / 1e4)
                stale = now - pos["t"] > MR_MAX_HOLD_SEC
                if (bid >= mean or stopped or stale) and portfolio.can_sell(sym, pos["held"]):
                    px = round(bid * (1 - SLIP_BPS / 1e4), 2)
                    self._mark_pending(sym)
                    logger.info("MR exit %s: sell %d @ %.2f (z=%.2f stop=%s stale=%s)",
                                sym, pos["held"], px, z, stopped, stale)
                    return Signal(symbol=sym, side="sell", quantity=pos["held"], price=px)
                continue

            buy_px = round(ask * (1 + SLIP_BPS / 1e4), 2)
            if z < -MR_Z and mean / buy_px - 1 > MR_MIN_EDGE_BPS / 1e4:
                qty = min(MR_SHARES, self._room(portfolio, sym), MAX_ORDER_SHARES)
                if qty > 0 and portfolio.can_buy(sym, qty, buy_px):
                    self._mr[sym] = {"held": 0, "entry": 0.0, "t": now}
                    self._mark_pending(sym)
                    logger.info("MR entry %s: buy %d @ %.2f (z=%.2f mean=%.2f)",
                                sym, qty, buy_px, z, mean)
                    return Signal(symbol=sym, side="buy", quantity=qty, price=buy_px)
        return None

        

    def _reconcile(self, market, portfolio) -> None:
        # Adopt the exchange's position when ours disagrees on two checks in a
        # row: granted shares, IPO allocations, fills missed while disconnected.
        for sym in set(self._books) | set(portfolio.positions):
            b = self._books.setdefault(sym, [0, 0.0, 0.0])
            server = portfolio.positions.get(sym, 0)
            if b[0] == server:
                self._mismatch.discard(sym)
            elif sym in self._mismatch:
                logger.warning("RECON %s mine=%d exchange=%d — adopting exchange", sym, b[0], server)
                if server == 0:
                    b[1] = 0.0
                elif b[0] == 0:
                    b[1] = self._ipo_px.get(sym) or market.mid_price(sym) or 0.0
                b[0] = server
                self._mismatch.discard(sym)
            else:
                self._mismatch.add(sym)

    def _log_status(self, market, portfolio) -> None:
        real = unreal = 0.0
        for sym, (pos, avg, r) in sorted(self._books.items()):
            mid = market.mid_price(sym) or avg
            u = pos * (mid - avg)
            real, unreal = real + r, unreal + u
            if pos or r:
                logger.info("STATUS %s pos=%d avg=%.4f realized=%+.2f unrealized=%+.2f",
                            sym, pos, avg, r, u)
        logger.info("STATUS mine realized=%+.2f unrealized=%+.2f | exchange realized=%+.2f "
                    "fees=%.2f cash=%.2f", real, unreal, portfolio.realized_pnl,
                    portfolio.total_fees_paid, portfolio.cash)

    def _trade_starter(self, market, portfolio):
        """Buy STARTER_QTY of the starter's symbol, hold STARTER_HOLD_SEC, sell
        it back. IOCs that miss are retried on a later tick."""
        s, sym = self._starter, self._starter["symbol"]
        now = time.monotonic()
        bid, ask = market.best_bid(sym), market.best_ask(sym)
        if (bid is None or ask is None or now < s["wait_until"] or self._is_pending(sym)
                or self._is_halted(sym)):
            return None

        if s["phase"] == "buy":
            price = round(ask * (1 + SLIP_BPS / 1e4), 2)
            if portfolio.can_buy(sym, STARTER_QTY, price):
                s["wait_until"] = now + ORDER_GAP_SEC
                self._mark_pending(sym)
                logger.info("Starter buy: %d %s @ %.2f", STARTER_QTY, sym, price)
                return Signal(symbol=sym, side="buy", quantity=STARTER_QTY, price=price)
            s["phase"] = "done"                   # can't afford even this: skip it
        elif s["phase"] == "hold" and now - s["filled_at"] >= STARTER_HOLD_SEC:
            s["phase"] = "sell"
        if s["phase"] == "sell":
            if s["held"] <= 0:
                s["phase"] = "done"
                logger.info("Starter done: position in %s closed", sym)
                return None
            price = round(bid * (1 - SLIP_BPS / 1e4), 2)
            if portfolio.can_sell(sym, s["held"]):
                s["wait_until"] = now + ORDER_GAP_SEC
                self._mark_pending(sym)
                logger.info("Starter sell: %d %s @ %.2f", s["held"], sym, price)
                return Signal(symbol=sym, side="sell", quantity=s["held"], price=price)
        return None

    def _trade_shock(self, s, market, portfolio):
        """One step of the earnings play. Good news: buy on the first ticks,
        sell into the overshoot, then short the peak and cover on the settle.
        Bad news: dump what we hold and go short on the first ticks, cover
        near the overshoot low, then buy the low and sell the recovery."""
        now = time.monotonic()
        sym, pct = s["symbol"], s["pct"]
        bid, ask = market.best_bid(sym), market.best_ask(sym)
        if s["ref_mid"] is None:
            s["ref_mid"] = market.mid_price(sym)  # no mid seen before the print
        # One side of the book can be empty (long-only: market makers can't
        # offer shares they don't own, so a shock can lift every ask). Buys
        # need an ask and sells a bid; never let a missing side freeze the play.
        if (not s["ref_mid"] or now < s["wait_until"] or self._is_pending(sym)
                or (bid is None and ask is None)):
            return None

        ref, ramp = s["ref_mid"], s["ramp_ticks"]
        elapsed = (now - s["fired"]) / EXCHANGE_TICK_SEC   # exchange ticks since the print
        peak = ref * (1 + s["overshoot"] * pct)
        settle = ref * (1 + pct)
        tick_move_bps = abs(pct) * s["overshoot"] / max(1, ramp) * 1e4
        slip = min(SHOCK_SLIP_MAX_BPS, max(SLIP_BPS, SHOCK_SLIP_TICK_FRACTION * tick_move_bps))
        buy_px = round(ask * (1 + slip / 1e4), 2) if ask is not None else None
        sell_px = round(bid * (1 - slip / 1e4), 2) if bid is not None else None

        pos = portfolio.positions.get(sym, 0)

        if pct > 0:
            if s["phase"] == "open":
                # Ride the ramp: exit near the peak, or when the ramp is over.
                if s["held"] > 0 and sell_px and (bid >= ref + EXIT_FRACTION * (peak - ref)
                                                  or elapsed >= ramp):
                    return self._shock_order(s, sym, "sell", s["held"], sell_px, "exit ramp")
                leftover_room = self._room(portfolio, sym)
                if (leftover_room > 0 and buy_px and elapsed < ramp / 2
                        and peak / buy_px - 1 > MIN_EDGE_BPS / 1e4):
                    qty = min(leftover_room, int(portfolio.cash * 0.95 / buy_px))
                    if qty > 0 and portfolio.can_buy(sym, qty, buy_px):
                        return self._shock_order(s, sym, "buy", qty, buy_px, "ride ramp")
                if s["held"] > 0 or elapsed < ramp / 2:
                    return None
                if not (FADE_POSITIVE and peak / settle - 1 > MIN_EDGE_BPS / 1e4):
                    self._end_shock(sym)
                    return None
                s["phase"] = "top"

            # Short the overshoot peak, cover once it settles back.
            if pos < 0:
                if buy_px and (ask <= settle * (1 + SLIP_BPS / 1e4) or elapsed >= ramp * 1.5 + 2):
                    return self._cover(s, sym, -pos, buy_px, portfolio, "cover top")
                return None
            want = pos + self._short_target()
            if (want > 0 and sell_px and ramp - 2 <= elapsed < ramp + 2 and not s["shorted"]
                    and self._may_short(s, sym, bid)
                    and sell_px / settle - 1 > MIN_EDGE_BPS / 1e4):
                s["shorted"] = True               # one entry; a partial fill is still covered
                return self._shock_order(s, sym, "sell", want, sell_px, "short top")
            if elapsed >= ramp + 2:
                self._end_shock(sym)
            return None

        # Bad news. Step 1: sell everything we hold, granted shares included,
        # and go short while the drop still has room to run.
        if s["phase"] == "open":
            want = max(pos, 0)
            if (sell_px and elapsed < ramp / 2 and self._may_short(s, sym, bid)
                    and sell_px / peak - 1 > MIN_EDGE_BPS / 1e4):
                want = pos + self._short_target()
            if want > 0 and sell_px:
                return self._shock_order(s, sym, "sell", want, sell_px,
                                         "short" if want > max(pos, 0) else "dump")
            s["phase"] = "cover"

        # Step 2: cover the short near the overshoot low (or when the ramp ends).
        if s["phase"] == "cover":
            if pos < 0:
                if buy_px and (ask <= ref + EXIT_FRACTION * (peak - ref) or elapsed >= ramp):
                    return self._cover(s, sym, -pos, buy_px, portfolio, "cover low")
                return None
            bounce = settle / peak - 1            # overshoot low back to settle
            if FADE_NEGATIVE and bounce > MIN_EDGE_BPS / 1e4:
                s["phase"] = "fade"
            elif not s["acted"]:
                s["phase"], s["token"] = "fade", True   # react anyway, with a tiny size
            else:
                s["phase"] = "done"

        # Step 2: buy near the overshoot low, sell the recovery.
        if s["phase"] == "fade":
            if s["held"] > 0 and sell_px and (bid >= settle * (1 - SLIP_BPS / 1e4)
                                              or elapsed >= ramp * 1.5 + 2):
                return self._shock_order(s, sym, "sell", s["held"], sell_px, "exit fade")
            leftover_room = self._room(portfolio, sym)
            if s["token"]:                        # one small buy, then just exit it
                leftover_room = 0 if s["bought"] else min(leftover_room, TOKEN_SHARES)
            if (leftover_room > 0 and buy_px and ramp - 2 <= elapsed < ramp + 2
                    and (s["token"] or settle / buy_px - 1 > MIN_EDGE_BPS / 1e4)):
                qty = min(leftover_room, int(portfolio.cash * 0.95 / buy_px))
                if qty > 0 and portfolio.can_buy(sym, qty, buy_px):
                    return self._shock_order(s, sym, "buy", qty, buy_px, "fade low")
            if s["held"] == 0 and elapsed >= ramp + 2:
                s["phase"] = "done"

        if s["phase"] == "done":
            self._end_shock(sym)
        return None

    def _room(self, portfolio, sym):
        """Shares we may still buy in sym without nearing the exchange cap."""
        return self._seat_caps(sym)[0] - portfolio.positions.get(sym, 0)

    def _short_target(self):
        """Most shares to be short in one symbol. Only the broker can be short
        alongside us (mean reversion is long-only)."""
        return max(0, min(SHORT_SHARES, self._limit - POSITION_BUFFER - BROKER_MAX_SHORT))

    # ── Hard limits ─────────────────────────────────────────────────────
    def _seat_caps(self, sym):
        """(most shares long, most shares short) THIS seat may hold in sym.
        Shock seat + mean-reversion seat + broker add up to at most
        position_limit − POSITION_BUFFER."""
        if "shock" in self._roles:
            broker = 0 if sym in BROKER_SKIP_SYMBOLS else BROKER_MAX_SHARES
            long_cap = min(SHOCK_SHARES, self._limit - POSITION_BUFFER - broker - OTHER_SEAT_SHARES)
            return max(0, long_cap), self._short_target()
        if "mr" in self._roles:
            return OTHER_SEAT_SHARES, 0
        return (STARTER_QTY if "starter" in self._roles else 0), 0

    def _mark(self, sym, market):
        return (self._ref.get(sym) or market.mid_price(sym)
                or self._books.get(sym, [0, 0.0])[1] or 0.0)

    def _equity(self, market, portfolio):
        """Net worth marked at the venue reference — what the exchange's
        maintenance check uses."""
        return portfolio.cash + sum(q * self._mark(s, market)
                                    for s, q in portfolio.positions.items() if q)

    def _capital(self):
        """What the venue's maintenance check is measured against: the
        registered allocation, or the first net worth the leaderboard showed
        this session if that is lower (a local venue, or a seat that lost
        money in an earlier session)."""
        capital = float(CAPITAL.get(_trader_config.TEAM_ID, _trader_config.STARTING_CASH))
        return capital if self._start_equity is None else min(capital, self._start_equity)

    def _risk_off_level(self):
        return self._capital() * (self._maint + RISK_OFF_MARGIN)

    def _risk_off_signal(self, market, portfolio):
        """Near the liquidation line: IOC out of the biggest position."""
        held = sorted(((abs(q) * self._mark(s, market), s, q)
                       for s, q in portfolio.positions.items()
                       if q and not self._is_halted(s) and not self._is_pending(s)),
                      reverse=True)
        for _, sym, qty in held:
            bid, ask = market.best_bid(sym), market.best_ask(sym)
            if qty > 0 and bid:
                px, side = round(bid * (1 - SLIP_BPS / 1e4), 2), "sell"
            elif qty < 0 and ask:
                px, side = round(ask * (1 + SLIP_BPS / 1e4), 2), "buy"
            else:
                continue
            self._mark_pending(sym)
            logger.warning("RISK OFF (equity under %.0f): %s %d %s @ %.2f",
                           self._risk_off_level(), side, abs(qty), sym, px)
            return Signal(symbol=sym, side=side, quantity=min(abs(qty), MAX_ORDER_SHARES), price=px)
        return None

    def _gate(self, sig, market, portfolio):
        """Shrink or drop an order so it can't break a hard limit (see the
        week 5 block at the top). Every order passes here before it is sent."""
        sym, side, qty, px = sig.symbol, sig.side, sig.quantity, sig.price
        if self._liquidated:
            return self._blocked(sig, "account liquidated")
        if self._is_halted(sym):
            return self._blocked(sig, "symbol halted")
        pos = portfolio.positions.get(sym, 0)
        flying = self._in_flight(sym, side)
        max_long, max_short = self._seat_caps(sym)
        team_cap = self._limit - POSITION_BUFFER
        team = pos + self._team_others.get(sym, 0)
        if side == "buy":
            room = min(max_long - pos, team_cap - team) - flying
            shrinks = max(0, -pos) - flying       # covering a short
        else:
            room = min(pos + max_short, team + team_cap) - flying
            shrinks = max(0, pos) - flying        # selling a long
        allowed = shrinks if self._risk_off else max(room, shrinks)
        adds = min(qty, allowed) - max(0, shrinks)
        if adds > 0 and not self._risk_off:       # the part that grows the book
            mark = self._mark(sym, market) or px
            gross = sum(abs(q) * self._mark(s, market) for s, q in portfolio.positions.items())
            spare = MAX_GROSS_LEVERAGE * self._equity(market, portfolio) - gross
            allowed = min(allowed, max(0, shrinks) + max(0, int(spare // mark)))
        qty = min(qty, allowed, MAX_ORDER_SHARES)
        if qty <= 0:
            return self._blocked(sig, "risk off: reduce-only" if self._risk_off else
                                 f"limit: pos={pos} in flight={flying} "
                                 f"caps=+{max_long}/-{max_short} team={team}")
        ref = self._ref.get(sym)
        if ref and self._luld > 0:                # LULD collar around the venue mark
            band = max(0.0, self._luld - LULD_MARGIN_PCT)
            px = (min(px, int(ref * (1 + band) * 100) / 100) if side == "buy"
                  else max(px, -int(-ref * (1 - band) * 100) / 100))
        if qty != sig.quantity or px != sig.price:
            logger.info("Gate: %s %s %d @ %.2f -> %d @ %.2f", side, sym,
                        sig.quantity, sig.price, qty, px)
        return Signal(symbol=sym, side=side, quantity=qty, price=px)

    def _blocked(self, sig, why):
        logger.info("Gate blocked %s %d %s: %s", sig.side, sig.quantity, sig.symbol, why)
        return None

    def _in_flight(self, sym, side):
        """Shares of IOCs we sent in sym/side whose fills may not have landed."""
        now = time.monotonic()
        self._inflight = [f for f in self._inflight if now - f[0] < INFLIGHT_SEC and f[3] > 0]
        return sum(f[3] for f in self._inflight if f[1] == sym and f[2] == side)

    def _note_sent(self, sig):
        self._inflight.append([time.monotonic(), sig.symbol, sig.side, sig.quantity])

    # ── Halts ───────────────────────────────────────────────────────────
    def _is_halted(self, sym):
        now = time.monotonic()
        if now < self._market_halt_until:
            return True
        until = self._halted.get(sym)
        if until is None:
            return False
        if now < until + HALT_SLACK_SEC:
            return True
        logger.warning("No TRADING_RESUMED for %s — assuming it reopened", sym)
        self._resumed(sym)
        return False

    def _resumed(self, sym):
        """A halted symbol reopened: keep mean reversion out while its history
        holds the jump, and queue a probe so the bot trades it again."""
        self._halted.pop(sym, None)
        self._paused[sym] = time.monotonic() + ON_EVENT_TIME
        if "starter" in self._roles and sym not in SKIP_SYMBOLS and sym not in self._probes:
            self._probes.append(sym)
        logger.info("RESUMED %s — trading it again%s", sym,
                    " (unwinding the shock position)" if sym in self._shocks else "")

    def _may_short(self, s, sym, bid):
        """True if a new short in sym should get past the venue: shorts are
        unlocked, no short was rejected this shock, and the symbol is not under
        (or about to trip) the short-sale rule."""
        if not self._shorts_ok or s["no_short"] or sym in self._ssr or bid is None:
            return False
        if (self._short_interest.get(sym, 0) + self._short_target() + BORROW_HEADROOM
                > self._locate_cap):
            return False
        open_mid = self._open_mid.get(sym) or s["ref_mid"]
        return bid > open_mid * (1 - self._ssr_pct + SSR_MARGIN_PCT)

    def _cover(self, s, sym, short, buy_px, portfolio, why):
        qty = min(short, int(portfolio.cash * 0.95 / buy_px))
        if qty > 0 and portfolio.can_buy(sym, qty, buy_px):
            return self._shock_order(s, sym, "buy", qty, buy_px, why)
        return None

    def on_open(self, data):
        """SESSION_OPEN: adopt the week's rules from the scenario it carries."""
        sc = data.get("scenario") or {}
        self._shorts_ok = bool((sc.get("flags") or {}).get("shorts_allowed", False))
        self._limit = int(sc.get("position_limit") or self._limit)
        cfg = sc.get("config") or {}
        self._ssr_pct = float(cfg.get("SSR_TRIGGER_PCT", self._ssr_pct))
        self._locate_cap = int(cfg.get("SHORT_LOCATE_CAP", self._locate_cap))
        self._luld = float(cfg.get("LULD_BAND_PCT", self._luld))
        self._maint = float(sc.get("maintenance_fraction") or self._maint)
        if not data.get("late_join"):             # SSR is sticky until the next open
            self._ssr.clear()
            self._open_mid.clear()
            self._halted.clear()
            self._market_halt_until = 0.0
            self._liquidated = False
            self._start_equity = None
        long_cap, short_cap = self._seat_caps("")
        logger.info("Session rules: shorts=%s limit=%d SSR=%.1f%% LULD=%.1f%% | seat caps "
                    "+%d/-%d per symbol, reduce-only under equity %.0f (liquidation %.0f)",
                    self._shorts_ok, self._limit, self._ssr_pct * 100, self._luld * 100,
                    long_cap, short_cap, self._risk_off_level(),
                    self._capital() * self._maint)

    def on_reject(self, code, message):
        logger.warning("REJECT [%s]: %s", code, message)
        if code in SHORT_REJECTS:                 # one reject per shock, never a streak
            for s in self._shocks.values():
                s["no_short"] = True
        elif code == "SYMBOL_HALTED":             # a halt we never heard about (reconnect)
            m = re.match(r"(\S+) trading is halted\. Resume in (\d+)s", message or "")
            if m:
                self._halted[m.group(1)] = time.monotonic() + int(m.group(2))
        elif code == "LIQUIDATED":
            self._liquidated = True

    def _is_pending(self, sym):
        """True while an order in sym may still have fills on the way, so
        portfolio.positions can't be trusted to size the next one."""
        return time.monotonic() < self._pending.get(sym, 0.0)

    def _mark_pending(self, sym):
        self._pending[sym] = time.monotonic() + PENDING_TIMEOUT_SEC

    def _shock_order(self, s, sym, side, qty, price, why):
        qty = min(qty, MAX_ORDER_SHARES)          # the rest goes on a later tick
        s["wait_until"] = time.monotonic() + ORDER_GAP_SEC
        s["acted"] = True
        s["bought"] = s["bought"] or side == "buy"
        self._mark_pending(sym)
        logger.info("Shock %s: %s %d %s @ %.2f", why, side, qty, sym, price)
        return Signal(symbol=sym, side=side, quantity=qty, price=price)

    def _end_shock(self, sym):
        s = self._shocks.pop(sym, None)
        if s:
            logger.info("Shock done: %s %+.2f%%", sym, s["pct"] * 100)

    def on_fill(self, side, symbol, quantity, price):
        if self._is_pending(symbol):             # an IOC fills at once; let its other fills land
            self._pending[symbol] = min(self._pending[symbol], time.monotonic() + FILL_SETTLE_SEC)
        left = quantity                          # these shares are no longer in flight
        for f in self._inflight:
            if f[1] == symbol and f[2] == side and left > 0:
                take = min(left, f[3])
                f[3] -= take
                left -= take
        s = self._shocks.get(symbol)
        if s:                                    # shares the shock play owns (a buy
            if side == "buy":                    # covers any short before adding)
                covered = min(quantity, max(0, -self._books.get(symbol, [0])[0]))
                s["held"] += quantity - covered
            else:
                s["held"] = max(0, s["held"] - quantity)
        st = self._starter
        if st["phase"] != "done" and symbol == st["symbol"]:
            if side == "buy":
                st["held"] += quantity
                if st["phase"] == "buy":          # first fill starts the hold clock
                    st["phase"], st["filled_at"] = "hold", time.monotonic()
            else:
                st["held"] = max(0, st["held"] - quantity)
        #shares that the mean reversion strategy owns
        m = self._mr.get(symbol)
        if m is not None:
            if side == "buy":
                m["entry"] = (m["entry"] * m["held"] + price * quantity) / (m["held"] + quantity)
                m["held"] += quantity
            else:
                m["held"] = max(0, m["held"] - quantity)
                if m["held"] == 0:
                    self._mr.pop(symbol)
        
        book = self._books.setdefault(symbol, [0, 0.0, 0.0])   # [position, avg_entry, realized]
        pos, avg, _ = book
        signed = quantity if side == "buy" else -quantity
        if pos == 0 or (pos > 0) == (signed > 0):
            book[1] = (avg * abs(pos) + price * quantity) / (abs(pos) + quantity)
        else:
            closing = min(quantity, abs(pos))
            book[2] += closing * (price - avg) * (1 if pos > 0 else -1)
            if quantity > closing:
                book[1] = price          # flipped through zero
            elif pos + signed == 0:
                book[1] = 0.0
        book[0] = pos + signed
        logger.info("BOOK %s pos=%d avg=%.4f realized=%+.2f", symbol, book[0], book[1], book[2])

    # shocks, calendar
    def on_event(self, event, message, data):
        logger.info("Event: %s | %s | %s", event, message, data)
        now = time.monotonic()
        if event == "TRADING_HALT" and data.get("symbol"):
            sym = data["symbol"]
            # Timed on our clock from the duration: the venue's resume_at is
            # its wall clock, which can be skewed from ours.
            self._halted[sym] = now + float(data.get("duration") or HALT_FALLBACK_SEC)
            logger.warning("HALT %s for %.0fs — no orders there; %s", sym,
                           self._halted[sym] - now,
                           "holding the shock position through it" if sym in self._shocks
                           else "everything else keeps trading")
            return
        if event == "TRADING_RESUMED" and data.get("symbol"):
            self._resumed(data["symbol"])
            return
        if event == "MARKET_HALT":
            self._market_halt_until = now + float(data.get("duration") or 3 * HALT_FALLBACK_SEC)
            logger.warning("MARKET HALT for %.0fs — no orders anywhere",
                           self._market_halt_until - now)
            return
        if event == "MARKET_RESUMED":
            self._market_halt_until = 0.0
            for sym in self._last_mid:
                self._paused[sym] = now + ON_EVENT_TIME
            self._resumed(STARTER_SYMBOL)
            return
        if event == "LIQUIDATION" and data.get("team_id") == _trader_config.TEAM_ID:
            self._liquidated = True
            logger.error("This seat was LIQUIDATED — no further orders this session")
            return
        if event == "CALENDAR_EVENT" and data.get("market_wide"):
            # An econ print ramps every symbol: mean reversion would buy each
            # leg of a move that keeps going.
            for sym in data.get("affected") or list(self._last_mid):
                self._paused[sym] = now + ON_EVENT_TIME
            logger.info("Market-wide %s: pausing mean reversion %ds",
                        data.get("kind", "event"), ON_EVENT_TIME)
        if event == "CALENDAR_EVENT" and data.get("kind") == "earnings":
            sym, pct = data.get("symbol"), data.get("pct")
            if not sym or not isinstance(pct, (int, float)) or pct == 0:
                logger.warning("Earnings event without a usable symbol/pct — not trading it: %s", data)
                return
            self._ramp = (data.get("ramp_ticks", self._ramp[0]),
                          data.get("overshoot", self._ramp[1]))
            self._start_shock(sym, pct)
            return
        if event == "SHOCK":
            # Teacher-injected: data["prices"] is each affected symbol's
            # target price; the move ramps in just like an earnings print.
            for sym, target in (data.get("prices") or {}).items():
                # The target is a fundamental: compare it with the venue mark,
                # not a book mid that may be stale (every untouched symbol
                # would read as a shock of its own).
                mid = self._ref.get(sym) or self._last_mid.get(sym)
                if not mid or not isinstance(target, (int, float)):
                    continue
                pct = target / mid - 1
                if abs(pct) >= MIN_SHOCK_PCT:
                    self._start_shock(sym, pct)
            return
        if event == "SSR_ON" and data.get("symbol"):
            self._ssr.add(data["symbol"])
        if event.startswith("IPO_") and data.get("symbol") and data.get("price"):
            self._ipo_px[data["symbol"]] = float(data["price"])
        ticker = data.get("symbol") if isinstance(data, dict) else None
        if ticker:
            self._paused[ticker] = time.monotonic() + ON_EVENT_TIME

    def _start_shock(self, sym, pct):
        if "shock" not in self._roles:
            # Another seat trades the print; keep mean reversion out of the
            # ramp, which looks like a huge z-score but keeps going.
            self._paused[sym] = time.monotonic() + ON_EVENT_TIME
            logger.info("Shock in %s: not this seat's play — pausing it", sym)
            return
        old = self._shocks.get(sym)                   # a second move in sym keeps its shares
        self._shocks[sym] = {"symbol": sym, "pct": pct,
                             "fired": time.monotonic(),
                             "ref_mid": self._last_mid.get(sym),   # mid before the ramp
                             "ramp_ticks": self._ramp[0],
                             "overshoot": self._ramp[1],
                             "held": old["held"] if old else 0,
                             "phase": "open", "wait_until": 0.0,
                             "acted": False, "token": False, "bought": False,
                             "shorted": False, "no_short": False}
        logger.info("Shock start: %s %+.2f%%", sym, pct * 100)

    def on_ipo(self, symbol, lo, hi, shares, data):
    #       # An IPO book opened — return qty (bids top of range) or
    #       # (qty, max_price) as your indication, None to pass.
        logger.info("IPO for %s at range %.2f-%.2f, %s shares | %s", symbol, lo, hi, shares, data)
        if "ipo" not in self._roles:
            return None
        cash =self._cash or _trader_config.STARTING_CASH   # on_ipo can fire before the first tick
        qty = min(shares, IPO_SHARES, int(cash * 0.10 / hi)) if hi > 0 else 0
        if qty <= 0:
            logger.info("IPO %s: PASS — 10%% of cash (%.2f) can't buy one share at %.2f",
                        symbol, cash, hi)
            return None
        logger.info("IPO %s: bidding %d at top of range %.2f — sized so a 100%% pro-rata "
                    "fill is <=10%% of cash; we'd rather miss upside than be overloaded "
                    "if the listing breaks", symbol, qty, hi)
        return qty

    def run(self) -> None:
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s  %(levelname)-8s  %(message)s",
                            datefmt="%H:%M:%S")
        asyncio.run(TakerBot(self).run())


def new_starter(symbol, wait_until=0.0) -> dict:
    """A buy-hold-sell round trip in symbol (the starter, or a resume probe)."""
    return {"phase": "buy", "symbol": symbol, "held": 0, "filled_at": 0.0,
            "wait_until": wait_until}


def team_positions(leaderboard, exclude) -> dict[str, int]:
    """Per-symbol position of our team's seats other than `exclude` — the
    venue's position limit counts all of them together."""
    out: dict[str, int] = {}
    for entry in list(leaderboard.traders or []) + list(leaderboard.brokers or []):
        tid = entry.get("team_id") or ""
        if tid.startswith(OWN_TEAM_PREFIX) and tid != exclude:
            for sym, qty in (entry.get("positions") or {}).items():
                out[sym] = out.get(sym, 0) + int(qty)
    return out


def short_interest(leaderboard) -> dict[str, int]:
    """Shares short per symbol across every seat on the leaderboard — what
    the venue's borrow cap is measured against."""
    out: dict[str, int] = {}
    for entry in list(leaderboard.traders or []) + list(leaderboard.brokers or []):
        for sym, qty in (entry.get("positions") or {}).items():
            if qty < 0:
                out[sym] = out.get(sym, 0) - int(qty)
    return out


class _AckTap:
    """Wraps the websocket so the bot sees order confirmations (OrderAck),
    which the stock bot only logs at DEBUG."""

    def __init__(self, ws, bot):
        self._ws, self._bot = ws, bot

    def __getattr__(self, name):
        return getattr(self._ws, name)

    def __aiter__(self):
        return self._tap()

    async def _tap(self):
        async for raw in self._ws:
            if '"order_ack"' in raw:
                try:
                    self._bot._on_ack(raw)
                except Exception:
                    logger.exception("ignoring unreadable order_ack")
            elif '"error"' in raw:
                try:
                    msg = json.loads(raw)
                    if msg.get("type") == "error":
                        self._bot._owner.on_reject(msg.get("code"), msg.get("message"))
                except Exception:
                    logger.exception("ignoring unreadable error")
            yield raw


class TakerBot(_AdaptedTraderBot):
    """The standard bot (on_tick → risk check → order), but every Signal is
    sent as an IOC order instead of a resting limit order."""

    def __init__(self, owner):
        super().__init__(owner)
        # Keep the venue mark (ref_price) per symbol: the LULD band and the
        # maintenance check are both measured from it, not from the book mid.
        update_book = self.market.update_book

        def update_book_and_ref(snapshot):
            update_book(snapshot)
            if snapshot.ref_price > 0:
                owner._ref[snapshot.symbol] = snapshot.ref_price
        self.market.update_book = update_book_and_ref

    async def connect(self):
        await super().connect()
        self._ws = _AckTap(self._ws, self)
        # The venue sends a PortfolioUpdate only on a seat's first connect of a
        # session, at the open and on fills, so after a reconnect cash and
        # positions are unknown until the first fill. Seed them from the
        # leaderboard (every ~2 s) until a real PortfolioUpdate arrives.
        self._portfolio_synced = False
        apply = type(self.portfolio).apply_server_update

        def synced_apply(update, _portfolio=self.portfolio):
            apply(_portfolio, update)
            self._portfolio_synced = True
        self.portfolio.apply_server_update = synced_apply

    def _on_ack(self, raw):
        ack = OrderAck.model_validate_json(raw)
        if ack.team_id != _trader_config.TEAM_ID:
            return
        logger.info("order_ack -> id: %s %s %d %s @ %.2f", ack.order_id, ack.side, ack.quantity, ack.symbol, ack.price)

    async def _on_session_event(self, msg) -> None:
        if msg.event in ("SESSION_PREOPEN", "SESSION_OPEN"):
            self._in_session = True
        elif msg.event == "SESSION_CLOSED":
            self._in_session = False
        await super()._on_session_event(msg)      # runs MyTrader.on_event

        # starter runs again when session opens to ensure we make a fill during session
        if msg.event == "SESSION_PREOPEN":
            self._owner._preopen = True
        elif msg.event == "SESSION_OPEN":
            self._owner._preopen = False
            self._owner.on_open(msg.data or {})
            self._owner._starter = new_starter(STARTER_SYMBOL)

        # React to an earnings print now instead of at the next tick (up to
        # TICK_INTERVAL_SEC later): the first fills go to whoever is fastest.
        if (msg.event in ("CALENDAR_EVENT", "SHOCK") and getattr(self._owner, "_shocks", None)
                and self._session_open):
            try:
                signal = self._owner.on_tick(self.market, self.portfolio)
                if signal is None:
                    return
                approved, reason = self.risk.check_order(
                    signal.symbol, signal.side, signal.quantity, signal.price,
                    self.portfolio, self.market)
                if approved:
                    await self._place_order(signal)
                else:
                    logger.info("Order blocked [%s]: %s", signal.symbol, reason)
            except Exception:
                logger.exception("on_tick raised on %s — waiting for the tick", msg.event)

    def _on_leaderboard(self, msg) -> None:
        # Reopen the trading gate if we reconnected mid-session.
        if getattr(self, "_in_session", False) and not self._session_open:
            self._session_open = True
            logger.info("Session still live after reconnect — trading gate reopened")

        super()._on_leaderboard(msg)
        self._owner._short_interest = short_interest(msg)
        self._owner._team_others = team_positions(msg, _trader_config.TEAM_ID)
        if self._owner._start_equity is None:
            me = next((e for e in msg.traders if e.get("team_id") == _trader_config.TEAM_ID), None)
            if me and "net_worth" in me:
                self._owner._start_equity = float(me["net_worth"])

        # Seed cash/positions after a reconnect (no PortfolioUpdate is resent).
        if getattr(self, "_portfolio_synced", True):
            return
        for entry in msg.traders:
            if entry.get("team_id") == _trader_config.TEAM_ID:
                self.portfolio.cash = float(entry.get("cash", self.portfolio.cash))
                self.portfolio.positions = {k: int(v) for k, v in
                                            (entry.get("positions") or {}).items()}
                self._portfolio_synced = True
                logger.info("Portfolio seeded from leaderboard: cash=%.2f positions=%s",
                            self.portfolio.cash, self.portfolio.positions)
                break

    async def flatten_all_positions(self) -> None:
        # The stock bot flattens on SESSION_CLOSED, but the exchange is already
        # closed by then, so every order comes back rejected (SESSION_CLOSED).
        # Hold positions through the close instead.
        logger.info("Session closed — holding positions (no flatten)")

    async def _place_order(self, signal: Signal) -> None:
        # trading_loop only guards on_tick, so a send that raised here would end
        # the trading task while the listener kept the bot "connected" but idle.
        # Log and drop the order instead; if the socket is gone, listen() ends
        # too and run() reconnects both tasks. (CancelledError is not an
        # Exception, so a reconnect can still cancel this task.)
        # Every order, from the tick loop or an event, passes the hard limits here.
        signal = self._owner._gate(signal, self.market, self.portfolio)
        if signal is None:
            return
        self._owner._note_sent(signal)
        try:
            await self._send(PlaceOrder(team_id=_trader_config.TEAM_ID,
                                        symbol=signal.symbol, side=signal.side,
                                        order_type="ioc", price=signal.price,
                                        quantity=signal.quantity))
        except Exception:
            logger.exception("Order send failed: %s %d %s — dropped",
                             signal.side, signal.quantity, signal.symbol)
            return
        logger.info("Order: IOC %s %d %s @ %.2f",
                    signal.side, signal.quantity, signal.symbol, signal.price)


if __name__ == "__main__":
    MyTrader().run()
