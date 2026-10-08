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
SHOCK_SHARES = 350        # most shares to buy into a shock (week-2 limit is 400)
SLIP_BPS = 20             # price IOCs this far through the touch so they fill
MIN_EDGE_BPS = 130        # enter only if this much move is left (2 taker fees + spread)
EXIT_FRACTION = 0.9       # sell once price covers this much of the move to the peak
FADE_NEGATIVE = True      # after bad news, buy the overshoot and sell the recovery
ORDER_GAP_SEC = 0.4       # wait this long after an order so its fill can arrive

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
        self._shock: dict | None = None           # the earnings shock being traded
        self._starter = {"phase": "buy", "held": 0, "filled_at": 0.0, "wait_until": 0.0}

    def on_tick(self, market, portfolio) -> Signal | None:
        for sym in market.symbols():
            mid = market.mid_price(sym)
            if mid:
                self._last_mid[sym] = mid

        # While an earnings shock is live it owns the bot.
        if self._shock:
            return self._trade_shock(market, portfolio)

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
        if bid is None or ask is None or now < s["wait_until"]:
            return None

        if s["phase"] == "buy":
            price = round(ask * (1 + SLIP_BPS / 1e4), 2)
            if portfolio.can_buy(sym, STARTER_QTY, price):
                s["wait_until"] = now + ORDER_GAP_SEC
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
                logger.info("Starter sell: %d %s @ %.2f", s["held"], sym, price)
                return Signal(symbol=sym, side="sell", quantity=s["held"], price=price)
        return None

    def _trade_shock(self, market, portfolio):
        """One step of the earnings play. Good news: buy on the first ticks
        and sell into the overshoot. Bad news (long-only): dump what we hold,
        then optionally buy the overshoot low and sell the recovery."""
        s = self._shock
        if s is None:
            return None
        now = time.monotonic()
        sym, pct = s["symbol"], s["pct"]
        bid, ask = market.best_bid(sym), market.best_ask(sym)
        if s["ref_mid"] is None:
            s["ref_mid"] = market.mid_price(sym)  # no mid seen before the print
        if bid is None or ask is None or not s["ref_mid"] or now < s["wait_until"]:
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
            leftover_room = SHOCK_SHARES - portfolio.positions.get(sym,0)
            if (leftover_room > 0 and elapsed < ramp / 2 and peak / buy_px - 1 > MIN_EDGE_BPS / 1e4):
                qty = min(leftover_room, int(portfolio.cash * 0.95 / buy_px))
                if qty > 0 and portfolio.can_buy(sym, qty, buy_px):
                    return self._shock_order(s, sym, "buy", qty, buy_px, "ride ramp")
            if s["held"] == 0 and elapsed >= ramp / 2:
                self._end_shock()
            return None

        # Bad news. Step 1: sell everything we hold, granted shares included.
        if s["phase"] == "dump":
            held = portfolio.positions.get(sym, 0)
            if held > 0 and portfolio.can_sell(sym, held):
                return self._shock_order(s, sym, "sell", held, sell_px, "dump")
            bounce = settle / peak - 1            # overshoot low back to settle
            s["phase"] = "fade" if FADE_NEGATIVE and bounce > MIN_EDGE_BPS / 1e4 else "done"

        # Step 2: buy near the overshoot low, sell the recovery.
        if s["phase"] == "fade":
            if s["held"] > 0 and (bid >= settle * (1 - SLIP_BPS / 1e4)
                                  or elapsed >= ramp * 1.5 + 2):
                return self._shock_order(s, sym, "sell", s["held"], sell_px, "exit fade")
            leftover_room = SHOCK_SHARES - portfolio.positions.get(sym,0)
            if (leftover_room > 0 and ramp - 2 <= elapsed < ramp + 2
                    and settle / buy_px - 1 > MIN_EDGE_BPS / 1e4):
                qty = min(leftover_room, int(portfolio.cash * 0.95 / buy_px))
                if qty > 0 and portfolio.can_buy(sym, qty, buy_px):
                    return self._shock_order(s, sym, "buy", qty, buy_px, "fade low")
            if s["held"] == 0 and elapsed >= ramp + 2:
                s["phase"] = "done"

        if s["phase"] == "done":
            self._end_shock()
        return None

    def _shock_order(self, s, sym, side, qty, price, why):
        s["wait_until"] = time.monotonic() + ORDER_GAP_SEC
        logger.info("Shock %s: %s %d %s @ %.2f", why, side, qty, sym, price)
        return Signal(symbol=sym, side=side, quantity=qty, price=price)

    def _end_shock(self):
        if self._shock:
            logger.info("Shock done: %s %+.2f%%", self._shock["symbol"], self._shock["pct"] * 100)
        self._shock = None

    def on_fill(self, side, symbol, quantity, price):
        s = self._shock
        if s and symbol == s["symbol"]:          # shares the shock play owns
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
            self._shock = {"symbol": sym, "pct": pct,
                           "fired": time.monotonic(),
                           "ref_mid": self._last_mid.get(sym),   # mid before the ramp
                           "ramp_ticks": data.get("ramp_ticks", 15),
                           "overshoot": data.get("overshoot", 1.3),
                           "held": 0, "phase": "dump", "wait_until": 0.0}
            return
        ticker = data.get("symbol") if isinstance(data, dict) else None
        if ticker:
            self._paused[ticker] = time.monotonic() + ON_EVENT_TIME

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
        if (msg.event == "CALENDAR_EVENT" and getattr(self._owner, "_shock", None)
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
                logger.exception("on_tick raised on CALENDAR_EVENT — waiting for the tick")
                
                
        

    async def _place_order(self, signal: Signal) -> None:
        await self._send(PlaceOrder(team_id=_trader_config.TEAM_ID,
                                    symbol=signal.symbol, side=signal.side,
                                    order_type="ioc", price=signal.price,
                                    quantity=signal.quantity))
        logger.info("Order: IOC %s %d %s @ %.2f",
                    signal.side, signal.quantity, signal.symbol, signal.price)


if __name__ == "__main__":
    MyTrader().run()
