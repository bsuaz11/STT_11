"""
STT_11 — broker (market maker).

Posts two-sided quotes and earns the spread plus maker rebates on every
passive fill. Three decisions, three methods — override any of them.
Inventory management (skew) is the key to staying solvent: the exchange
charges margin interest and liquidates teams below maintenance.

Run:
    TEAM_ID=stt_11_broker_1 python -m team.broker
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque

import broker.config as _broker_config
from shared.messages import PlaceOrder

from arena import Broker
from arena.broker import _AdaptedBrokerBot

logger = logging.getLogger(__name__)

# The venue allows 20 order/cancel messages per exchange tick (1 s). Requoting
# one symbol cancels its old ladder and posts a new one — up to 12 messages
# with the default 3 levels — and the requote loop runs every 0.5 s, so two
# symbols moving in the same second blew the quota (RATE_LIMITED). Spend at
# most this many messages in any rolling second, leaving headroom.
MESSAGE_BUDGET = 16

# Pricing. In the headless simulator at live fees (30 bps taker, 5 bps
# rebate), a 30 bps minimum width plus inventory skew beat the default
# quoting on every one of 5 seeds: wider quotes get picked off less, and the
# skew sheds inventory before a trend turns it into a loss. Replayed on 30
# minutes of the real public order book, reaching full skew at 100 shares
# instead of 300 did better (+$1,222 vs +$961) and lost on fewer symbols;
# 20 bps looked better on the replay but was the worst in the simulator.
MIN_SPREAD_BPS = 30   # never quote narrower than this (default width is ≤25 bps)
MAX_INVENTORY = 100   # shares at which the skew reaches a full half-spread


class _ErrorTap:
    """Wraps the websocket to notice SHORTS_LOCKED rejections, which the
    stock bot only logs."""

    def __init__(self, ws, bot):
        self._ws, self._bot = ws, bot

    def __getattr__(self, name):
        return getattr(self._ws, name)

    def __aiter__(self):
        return self._tap()

    async def _tap(self):
        async for raw in self._ws:
            if '"SHORTS_LOCKED"' in raw and not self._bot._shorts_locked:
                self._bot._shorts_locked = True
                logger.info("Short selling is locked this week — "
                            "quoting offers only for shares we hold")
            yield raw


class BudgetedBrokerBot(_AdaptedBrokerBot):
    """Standard quoting, but each pass stays under the venue's message quota.

    Symbols that need a requote are served most-urgent first (no quote at
    all, then biggest price move); whatever doesn't fit in this pass's budget
    waits for the next pass, 0.5s later.
    """

    def __init__(self, owner, exchange_url=None):
        super().__init__(owner, exchange_url)
        self._sent = deque()                    # send times, for the quota
        # Until week 4 a sell may only reduce a long. Once the venue says so,
        # cap each requote's offers at the shares we hold instead of sending
        # sells that bounce (each bounce also burns quota).
        self._shorts_locked = False
        self._sell_room = {}                    # symbol -> shares we may still offer

    async def connect_to_exchange(self):
        await super().connect_to_exchange()
        self._ws = _ErrorTap(self._ws, self)

    async def quote_symbol(self, symbol):
        if self._shorts_locked:
            self._sell_room[symbol] = max(0, self.state.positions.get(symbol, 0))
        await super().quote_symbol(symbol)
        self._sell_room.pop(symbol, None)

    async def _send(self, msg):
        if (isinstance(msg, PlaceOrder) and msg.side == "sell"
                and msg.symbol in self._sell_room):
            qty = min(msg.quantity, self._sell_room[msg.symbol])
            if qty <= 0:
                return                          # nothing left to offer
            self._sell_room[msg.symbol] -= qty
            msg = msg.model_copy(update={"quantity": qty})
        self._sent.append(time.monotonic())
        await super()._send(msg)

    def _room(self):
        """Messages we may still send without exceeding the per-second quota."""
        now = time.monotonic()
        while self._sent and now - self._sent[0] > 1.05:
            self._sent.popleft()
        return MESSAGE_BUDGET - len(self._sent)

    def _requote_cost(self, symbol: str) -> int:
        """Messages a requote of `symbol` sends: cancels + new ladder."""
        resting = self.state.resting_orders.get(symbol, {})
        cancels = 0
        for id_key, ids_key in (("buy_id", "buy_ids"), ("sell_id", "sell_ids")):
            ids = set(resting.get(ids_key) or [])
            if resting.get(id_key):
                ids.add(resting[id_key])
            cancels += len(ids)
        return cancels + 2 * _broker_config.QUOTE_LEVELS

    def _urgency(self, symbol: str, centre: float) -> float:
        """Higher = requote sooner. Unquoted symbols come first."""
        last = self.state.last_quote_prices.get(symbol)
        if last is None:
            return float("inf")
        return abs(centre - last) / centre

    async def requote_loop(self) -> None:
        while True:
            await asyncio.sleep(_broker_config.REQUOTE_INTERVAL_SEC)
            if not self._session_open:
                continue

            due = []
            for sym in list(self._quote_symbols):
                centre = self.quote_centre(sym)
                if centre and self.needs_requote(sym, centre):
                    due.append((self._urgency(sym, centre), sym))
            due.sort(reverse=True)

            budget = self._room()
            for _, sym in due:
                cost = self._requote_cost(sym)
                if cost > budget:
                    continue                    # next pass
                budget -= cost
                try:
                    await self.quote_symbol(sym)
                except Exception:
                    logger.exception("requote_loop error for %s", sym)


class MyBroker(Broker):
    """Your market-making logic. An empty subclass already quotes."""

    def __init__(self):
        super().__init__()
        self._half_spread = {}                    # symbol -> last half-width ($)

    def spread(self, symbol, price, history):
        # `history` is the recent reference prices, oldest first.
        # Return the dollar width, or None for the fixed default.
        if not price:
            return None
        width = max(min(0.30, price * 25 / 1e4),  # the default width
                    price * MIN_SPREAD_BPS / 1e4)
        self._half_spread[symbol] = width / 2
        return width

    def skew(self, symbol, inventory):
        # Shift both quotes against our inventory: long → quote lower so our
        # offer gets lifted first. At MAX_INVENTORY the shift is a full
        # half-spread, which puts the reducing side at the mid.
        half = self._half_spread.get(symbol)
        if not half or not inventory:
            return None
        ratio = max(-1.0, min(1.0, inventory / MAX_INVENTORY))
        return -ratio * half

    # Optional extra hooks:
    #   def toxic(self, trader_id): ...                 # Level 5
    #   def on_fill(self, side, symbol, quantity, price): ...

    async def _run_venues(self) -> None:
        # Same as Broker._run_venues, but with the budgeted bot.
        urls = [u.strip() for u in _broker_config.EXCHANGE_URLS if u.strip()]
        if len(urls) <= 1:
            await BudgetedBrokerBot(self, urls[0] if urls else None).run()
            return
        bots = [BudgetedBrokerBot(self, url) for url in urls]
        for b in bots[1:]:
            b.state.yahoo_prices = bots[0].state.yahoo_prices
            b._own_yahoo_feed = False
        await asyncio.gather(*(b.run() for b in bots))


if __name__ == "__main__":
    MyBroker().run()
