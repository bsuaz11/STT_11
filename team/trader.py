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
import logging
import time

import trader.config as _trader_config
from shared.messages import OrderAck, PlaceOrder, Signal

from arena import Trader
from arena.trader import _AdaptedTraderBot

logger = logging.getLogger(__name__)

# ── Strategy settings ────────────────────────────────────────────────────
# TODO: add your strategy parameters here.

# ── Earnings-shock settings ──────────────────────────────────────────────
# On CALENDAR_EVENT the exchange publishes the signed move (pct), then ramps
# fair value to overshoot × pct over ramp_ticks and settles back to 1.0 × pct
# over about half as many again (docs/SEASON_GUIDE.md §6).
EXCHANGE_TICK_SEC = 1.0   # the exchange advances prices once per second
SHOCK_SHARES = 550        # most shares to hold in a shock symbol
POSITION_LIMIT = 600      # exchange per-symbol cap (week 3: 600) — over it is rejected
POSITION_BUFFER = 20      # stay this far under POSITION_LIMIT
SLIP_BPS = 20             # price IOCs this far through the touch so they fill
MIN_EDGE_BPS = 130        # enter only if this much move is left (2 taker fees + spread)
EXIT_FRACTION = 0.9       # sell once price covers this much of the move to the peak
FADE_NEGATIVE = True      # after bad news, buy the overshoot and sell the recovery
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


class MyTrader(Trader):
    """Your alpha lives here."""

    def __init__(self):
        super().__init__()
        self._bought = {}                         # symbol -> (shares, avg price)
        self._paused = {} #keep track of what stocks are being paused atm
        self._last_mid = {}                       # symbol -> mid at the last tick
        self._shocks: dict[str, dict] = {}        # symbol -> earnings shock being traded
        self._pending: dict[str, float] = {}      # symbol -> time its in-flight order settles
        self._ramp = (15, 1.3)                    # (ramp_ticks, overshoot), updated from CALENDAR_EVENT
        self._starter = {"phase": "buy", "held": 0, "filled_at": 0.0, "wait_until": 0.0}

    def on_tick(self, market, portfolio) -> Signal | None:
        for sym in market.symbols():
            mid = market.mid_price(sym)
            if mid:
                self._last_mid[sym] = mid

        # While an earnings shock is live it owns the bot.
        if self._shocks:
            for sym in list(self._shocks):
                signal = self._trade_shock(self._shocks[sym], market, portfolio)
                if signal is not None:
                    return signal
            return None

        if self._starter["phase"] != "done":
            return self._trade_starter(market, portfolio)

        # implement your strategy.
        #
        # You can read:
        #   market.symbols()                    tradeable symbols (may include
        #                                       futures like ARENA10 — skip them)
        #   market.mid_price(sym)               current mid price
        #   market.prices(sym)                  recent mids, oldest first
        #   market.best_bid/best_ask(sym)       the touch
        #   portfolio.cash                      your cash
        #   portfolio.positions                 symbol -> shares held (includes
        #                                       the shares granted at the open)
        #   portfolio.can_buy(sym, qty, px)     / can_sell(sym, qty)
        #   self._bought[sym]                   (shares, avg price) from on_fill
        #   time.monotonic() < self._paused.get(sym, 0)
        #                                       True while on_event pauses sym
        #
        # Return a Signal(symbol=..., side="buy"/"sell", quantity=..., price=...)
        # to trade, or None to sit out this tick.

        for sym in market.symbols():
            # Filter tickers
            if time.monotonic() < self._paused.get(sym, 0):
                continue
            bid, ask = market.best_bid(sym), market.best_ask(sym)
            if bid is None or ask is None:
                continue


        return None

    def _trade_starter(self, market, portfolio):
        """Buy STARTER_QTY of STARTER_SYMBOL, hold STARTER_HOLD_SEC, sell it
        back. IOCs that miss are retried on a later tick."""
        s, sym = self._starter, STARTER_SYMBOL
        now = time.monotonic()
        bid, ask = market.best_bid(sym), market.best_ask(sym)
        if bid is None or ask is None or now < s["wait_until"] or self._is_pending(sym):
            return None

        if s["phase"] == "buy":
            price = round(ask * (1 + SLIP_BPS / 1e4), 2)
            if portfolio.can_buy(sym, STARTER_QTY, price):
                s["wait_until"] = now + ORDER_GAP_SEC
                self._mark_pending(sym)
                logger.info("Starter buy: %d %s @ %.2f", STARTER_QTY, sym, price)
                return Signal(symbol=sym, side="buy", quantity=STARTER_QTY, price=price)
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
        """One step of the earnings play. Good news: buy on the first ticks
        and sell into the overshoot. Bad news (long-only): dump what we hold,
        then optionally buy the overshoot low and sell the recovery."""
        now = time.monotonic()
        sym, pct = s["symbol"], s["pct"]
        bid, ask = market.best_bid(sym), market.best_ask(sym)
        if s["ref_mid"] is None:
            s["ref_mid"] = market.mid_price(sym)  # no mid seen before the print
        if (bid is None or ask is None or not s["ref_mid"] or now < s["wait_until"]
                or self._is_pending(sym)):
            return None

        ref, ramp = s["ref_mid"], s["ramp_ticks"]
        elapsed = (now - s["fired"]) / EXCHANGE_TICK_SEC   # exchange ticks since the print
        peak = ref * (1 + s["overshoot"] * pct)
        settle = ref * (1 + pct)
        buy_px = round(ask * (1 + SLIP_BPS / 1e4), 2)
        sell_px = round(bid * (1 - SLIP_BPS / 1e4), 2)

        if pct > 0:
            # Ride the ramp: exit near the peak, or when the ramp is over.
            if s["held"] > 0 and (bid >= ref + EXIT_FRACTION * (peak - ref)
                                  or elapsed >= ramp):
                return self._shock_order(s, sym, "sell", s["held"], sell_px, "exit ramp")
            leftover_room = self._room(portfolio, sym)
            if (leftover_room > 0 and elapsed < ramp / 2 and peak / buy_px - 1 > MIN_EDGE_BPS / 1e4):
                qty = min(leftover_room, int(portfolio.cash * 0.95 / buy_px))
                if qty > 0 and portfolio.can_buy(sym, qty, buy_px):
                    return self._shock_order(s, sym, "buy", qty, buy_px, "ride ramp")
            if s["held"] == 0 and elapsed >= ramp / 2:
                self._end_shock(sym)
            return None

        # Bad news. Step 1: sell everything we hold, granted shares included.
        if s["phase"] == "dump":
            held = portfolio.positions.get(sym, 0)
            if held > 0 and portfolio.can_sell(sym, held):
                return self._shock_order(s, sym, "sell", held, sell_px, "dump")
            bounce = settle / peak - 1            # overshoot low back to settle
            if FADE_NEGATIVE and bounce > MIN_EDGE_BPS / 1e4:
                s["phase"] = "fade"
            elif not s["acted"]:
                s["phase"], s["token"] = "fade", True   # react anyway, with a tiny size
            else:
                s["phase"] = "done"

        # Step 2: buy near the overshoot low, sell the recovery.
        if s["phase"] == "fade":
            if s["held"] > 0 and (bid >= settle * (1 - SLIP_BPS / 1e4)
                                  or elapsed >= ramp * 1.5 + 2):
                return self._shock_order(s, sym, "sell", s["held"], sell_px, "exit fade")
            leftover_room = self._room(portfolio, sym)
            if s["token"]:                        # one small buy, then just exit it
                leftover_room = 0 if s["bought"] else min(leftover_room, TOKEN_SHARES)
            if (leftover_room > 0 and ramp - 2 <= elapsed < ramp + 2
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
        cap = min(SHOCK_SHARES, POSITION_LIMIT - POSITION_BUFFER)
        return cap - portfolio.positions.get(sym, 0)

    def _is_pending(self, sym):
        """True while an order in sym may still have fills on the way, so
        portfolio.positions can't be trusted to size the next one."""
        return time.monotonic() < self._pending.get(sym, 0.0)

    def _mark_pending(self, sym):
        self._pending[sym] = time.monotonic() + PENDING_TIMEOUT_SEC

    def _shock_order(self, s, sym, side, qty, price, why):
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
        s = self._shocks.get(symbol)
        if s:                                    # shares the shock play owns
            s["held"] = s["held"] + quantity if side == "buy" else max(0, s["held"] - quantity)
        st = self._starter
        if st["phase"] != "done" and symbol == STARTER_SYMBOL:
            if side == "buy":
                st["held"] += quantity
                if st["phase"] == "buy":          # first fill starts the hold clock
                    st["phase"], st["filled_at"] = "hold", time.monotonic()
            else:
                st["held"] = max(0, st["held"] - quantity)
        shares, avg = self._bought.get(symbol, (0, 0.0))
        if side == "buy":
            total = shares + quantity
            self._bought[symbol] = (total, (shares * avg + quantity * price) / total)
        elif shares > quantity:
            self._bought[symbol] = (shares - quantity, avg)
        else:
            self._bought.pop(symbol, None)

    # shocks, calendar
    def on_event(self, event, message, data):
        logger.info("Event: %s | %s | %s", event, message, data)
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
                mid = self._last_mid.get(sym)
                if not mid or not isinstance(target, (int, float)):
                    continue
                pct = target / mid - 1
                if abs(pct) >= MIN_SHOCK_PCT:
                    self._start_shock(sym, pct)
            return
        ticker = data.get("symbol") if isinstance(data, dict) else None
        if ticker:
            self._paused[ticker] = time.monotonic() + ON_EVENT_TIME

    def _start_shock(self, sym, pct):
        old = self._shocks.get(sym)                   # a second move in sym keeps its shares
        self._shocks[sym] = {"symbol": sym, "pct": pct,
                             "fired": time.monotonic(),
                             "ref_mid": self._last_mid.get(sym),   # mid before the ramp
                             "ramp_ticks": self._ramp[0],
                             "overshoot": self._ramp[1],
                             "held": old["held"] if old else 0,
                             "phase": "dump", "wait_until": 0.0,
                             "acted": False, "token": False, "bought": False}
        logger.info("Shock start: %s %+.2f%%", sym, pct * 100)

    def on_ipo(self, symbol, lo, hi, shares, data):
    #       # An IPO book opened — return qty (bids top of range) or
    #       # (qty, max_price) as your indication, None to pass.
        logger.info("IPO for %s at range %.2f-%.2f, %s shares | %s", symbol, lo, hi, shares, data)

        #get as many ipo shares offered as possible
        qty = min(shares, IPO_SHARES)

        #capping at midpoint so i won't pay full price
        return (qty, round((lo + hi) / 2, 2))

    def run(self) -> None:
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s  %(levelname)-8s  %(message)s",
                            datefmt="%H:%M:%S")
        asyncio.run(TakerBot(self).run())


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
                self._bot._on_ack(raw)
            yield raw


class TakerBot(_AdaptedTraderBot):
    """The standard bot (on_tick → risk check → order), but every Signal is
    sent as an IOC order instead of a resting limit order."""

    async def connect(self):
        await super().connect()
        self._ws = _AckTap(self._ws, self)

    def _on_ack(self, raw):
        ack = OrderAck.model_validate_json(raw)
        if ack.team_id != _trader_config.TEAM_ID:
            return
        logger.info("order_ack -> id: %s %s %d %s @ %.2f", ack.order_id, ack.side, ack.quantity, ack.symbol, ack.price)

    async def _on_session_event(self, msg) -> None:
        await super()._on_session_event(msg)      # runs MyTrader.on_event
        # React to an earnings print now instead of at the next tick (up to
        # TICK_INTERVAL_SEC later): the first fills go to whoever is fastest.
        
        #starter runs again when session opens to ensure we make a fill during session
        if msg.event == "SESSION_OPEN":
            self._owner._starter = {"phase": "buy", "held": 0, "filled_at":0.0, "wait_until": 0.0}
        #only runs when there's a shock
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
                
                
        

    async def flatten_all_positions(self) -> None:
        # The stock bot flattens on SESSION_CLOSED, but the exchange is already
        # closed by then, so every order comes back rejected (SESSION_CLOSED).
        # Hold positions through the close instead.
        logger.info("Session closed — holding positions (no flatten)")

    async def _place_order(self, signal: Signal) -> None:
        await self._send(PlaceOrder(team_id=_trader_config.TEAM_ID,
                                    symbol=signal.symbol, side=signal.side,
                                    order_type="ioc", price=signal.price,
                                    quantity=signal.quantity))
        logger.info("Order: IOC %s %d %s @ %.2f",
                    signal.side, signal.quantity, signal.symbol, signal.price)


if __name__ == "__main__":
    MyTrader().run()
