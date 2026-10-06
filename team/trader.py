"""
STT_11 — trader bot.

Passive, long-only market maker. Spreads on the arena are wide (20–80 bps)
and the taker fee is 30 bps, so the money is in being the resting order
other people trade into — earning part of the spread plus the maker rebate —
never in crossing the spread:

  * a bid QUOTE_BPS below the current mid (inside the spread, never crossing)
  * once it holds shares, an offer at QUOTE_BPS above the mid, but never
    below TAKE_BPS over what it paid
  * a stop: if the price falls STOP_BPS below what it paid, sell at the bid
  * at most MAX_POSITION shares per stock, so a falling stock can't pile up

Replayed on 30 minutes of the arena's real public order book (fills when a
real trade prints through our price), this beat the earlier "buy 0.4% below
the 2-minute average" version in both halves of the recording: +$866 vs
+$200 overall. That version lost ~$3,400 live: its 1.5% stop against a 0.4%
target needed an ~80% win rate. One 30-minute sample is not proof — compare
the live P&L against the other traders before trusting it.

Run one seat:
    TEAM_ID=stt_11_trader_1 python -m team.trader
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque

import trader.config as _trader_config
from shared.messages import CancelOrder, OrderAck, PlaceOrder

from arena import Trader
from arena.trader import _AdaptedTraderBot

logger = logging.getLogger(__name__)

# ── Strategy settings ────────────────────────────────────────────────────
WARMUP = 20           # mid samples (10 s at 2/s) before quoting a symbol
QUOTE_BPS = 20        # rest our bid/offer this far from the current mid
TAKE_BPS = 40         # never offer below this much over what we paid
STOP_BPS = 100        # sell at the bid if price drops this far below what we paid
ORDER_QTY = 20        # shares per buy order
MAX_POSITION = 60     # most shares to hold in one symbol (week 1 limit is 200)

# ── Order-management settings ────────────────────────────────────────────
REPRICE_BPS = 10      # replace a resting order only if its price is this far off
ACK_TIMEOUT_SEC = 3   # forget an order the venue never confirmed after this long
MESSAGE_BUDGET = 16   # order/cancel messages per second (venue allows 20 per 1 s tick)


class MyTrader(Trader):
    """Your alpha lives here."""

    def __init__(self):
        super().__init__()
        self._bought = {}                         # symbol -> (shares, avg price)
        self._mids = {}                           # symbol -> recent mid prices

    def observe(self, market):
        """Sample every mid once per tick. The venue only sends a symbol's
        book when it changes, so market.prices() fills unevenly."""
        for sym in market.symbols():
            mid = market.mid_price(sym)
            if mid:
                self._mids.setdefault(sym, deque(maxlen=WARMUP)).append(mid)

    def orders_for(self, symbol, market, portfolio):
        """The orders we want resting in `symbol` now: [(side, qty, price)]."""
        history = self._mids.get(symbol, ())
        if len(history) < WARMUP:
            return []                             # not enough data yet
        mid = history[-1]
        held = portfolio.positions.get(symbol, 0)
        bid, ask = market.best_bid(symbol), market.best_ask(symbol)
        # Shares we were granted at connect have no purchase price; measure
        # them against the mid instead.
        cost = self._bought.get(symbol, (0, mid))[1]

        orders = []
        if held > 0:
            if bid and bid < cost * (1 - STOP_BPS / 1e4):
                return [("sell", held, bid)]      # stop out at the bid
            price = max(cost * (1 + TAKE_BPS / 1e4), mid * (1 + QUOTE_BPS / 1e4))
            orders.append(("sell", held, round(price, 2)))
        if held < MAX_POSITION:
            price = round(mid * (1 - QUOTE_BPS / 1e4), 2)
            if ask is None or price < ask:        # long-only, and never cross to buy
                orders.append(("buy", min(ORDER_QTY, MAX_POSITION - held), price))
        return orders

    def on_fill(self, side, symbol, quantity, price):
        shares, avg = self._bought.get(symbol, (0, 0.0))
        if side == "buy":
            total = shares + quantity
            self._bought[symbol] = (total, (shares * avg + quantity * price) / total)
        elif shares > quantity:
            self._bought[symbol] = (shares - quantity, avg)
        else:
            self._bought.pop(symbol, None)

    def on_tick(self, market, portfolio):
        return None                               # PassiveTraderBot uses orders_for()

    # Optional extra hooks:
    #   def on_event(self, event, message, data): ...   # shocks, calendar
    #   def on_ipo(self, symbol, lo, hi, shares, data): ...
    #       # An IPO book opened — return qty (bids top of range) or
    #       # (qty, max_price) as your indication, None to pass.

    def run(self) -> None:
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s  %(levelname)-8s  %(message)s",
                            datefmt="%H:%M:%S")
        asyncio.run(PassiveTraderBot(self).run())


# ── Plumbing: keep resting orders in line with orders_for() ──────────────

class _AckTap:
    """Wraps the websocket so the bot sees order confirmations (OrderAck),
    which carry the order ids needed to cancel — the stock bot drops them."""

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
            elif '"future"' in raw:               # e.g. ARENA10 — not a stock
                msg = json.loads(raw)
                if msg.get("type") == "book_snapshot" and msg.get("asset_type") != "equity":
                    self._bot._skip.add(msg["symbol"])
            yield raw


class PassiveTraderBot(_AdaptedTraderBot):
    """Each tick, cancel/replace resting orders that no longer match what
    the strategy wants, staying under the venue's message quota."""

    def __init__(self, owner):
        super().__init__(owner)
        self._open = {}                           # order_id -> [symbol, side, price, qty]
        self._unacked = {}                        # symbol -> [(side, price, qty, sent_at)]
        self._skip = set()                        # futures and other non-stocks
        self._sent = deque()                      # send times, for the message quota

    async def _send(self, msg):
        self._sent.append(time.monotonic())
        await super()._send(msg)

    def _room(self):
        """Messages we may still send without exceeding the per-second quota."""
        now = time.monotonic()
        while self._sent and now - self._sent[0] > 1.05:
            self._sent.popleft()
        return MESSAGE_BUDGET - len(self._sent)

    async def connect(self):
        await super().connect()
        # The venue cancels our resting orders when a connection drops.
        self._open.clear()
        self._unacked.clear()
        self._ws = _AckTap(self._ws, self)

    def _on_ack(self, raw):
        ack = OrderAck.model_validate_json(raw)
        if ack.team_id != _trader_config.TEAM_ID:
            return
        pending = self._unacked.get(ack.symbol, [])
        for i, (side, price, _qty, _t) in enumerate(pending):
            if side == ack.side and abs(price - ack.price) < 0.005:
                pending.pop(i)
                break
        self._open[ack.order_id] = [ack.symbol, ack.side, ack.price, ack.quantity]

    def _on_trade(self, msg):
        super()._on_trade(msg)
        me = _trader_config.TEAM_ID
        side = "buy" if msg.buyer_id == me else "sell" if msg.seller_id == me else None
        if side is None:
            return
        for oid, (sym, oside, price, qty) in list(self._open.items()):
            if sym == msg.symbol and oside == side and abs(price - msg.price) < 0.005:
                if qty <= msg.quantity:
                    del self._open[oid]
                else:
                    self._open[oid][3] = qty - msg.quantity
                break

    @staticmethod
    def _matches(resting, wanted):
        """True if the resting orders are close enough to what we want."""
        if len(resting) != len(wanted):
            return False
        for (side, qty, price), (wside, wqty, wprice) in zip(sorted(resting), sorted(wanted)):
            if side != wside or qty != wqty or abs(price - wprice) > wprice * REPRICE_BPS / 1e4:
                return False
        return True

    async def trading_loop(self) -> None:
        while True:
            await asyncio.sleep(_trader_config.TICK_INTERVAL_SEC)
            if not self._session_open:
                continue
            self._owner.observe(self.market)
            budget = self._room()
            now = time.monotonic()
            for sym in self.market.symbols():
                if sym in self._skip:
                    continue
                pending = [p for p in self._unacked.get(sym, [])
                           if now - p[3] < ACK_TIMEOUT_SEC]
                self._unacked[sym] = pending
                if pending:
                    continue                      # wait for the venue to confirm
                try:
                    wanted = self._owner.orders_for(sym, self.market, self.portfolio)
                except Exception:
                    logger.exception("orders_for raised for %s — skipping", sym)
                    continue
                mine = [oid for oid, o in self._open.items() if o[0] == sym]
                resting = [(self._open[oid][1], self._open[oid][3], self._open[oid][2])
                           for oid in mine]
                if self._matches(resting, wanted):
                    continue
                cost = len(mine) + len(wanted)
                if cost > budget:
                    continue                      # next tick
                budget -= cost
                for oid in mine:
                    await self._send(CancelOrder(team_id=_trader_config.TEAM_ID,
                                                 order_id=oid, symbol=sym))
                    del self._open[oid]
                for side, qty, price in wanted:
                    if qty <= 0 or price <= 0:
                        continue
                    await self._send(PlaceOrder(team_id=_trader_config.TEAM_ID,
                                                symbol=sym, side=side, order_type="limit",
                                                price=price, quantity=qty))
                    pending.append((side, price, qty, now))
                    logger.info("Order: %s %d %s @ %.2f", side, qty, sym, price)


if __name__ == "__main__":
    MyTrader().run()
