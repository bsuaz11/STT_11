"""
STT_11 — your exchange venue (port 8767).

The matching engine, settlement, and market data are provided. You set
policy: the fee schedule you publish, and the orders you accept.

Order policy (Phase 3, team/risk.py VenueGuard; RISK.md has the numbers):

    rate limit    a token bucket per seat, well above any normal bot, and a
                  short penalty box for a seat that keeps breaching it
    fat finger    order notional cap and a price collar around the last trade
    halts         orders in a halted symbol are refused here with a clear
                  reason; for REOPEN_TICKS after it reopens the collar is
                  tighter and market orders are refused, so the first prints
                  after a halt come from priced orders, not a stale sweep

House bots (the venue's own market makers) are exempt.

Run:
    EXCHANGE_PORT=8767 python -m team.exchange
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from typing import Any

import exchange.config as _config
import websockets
from exchange.server import _print_startup, bind_target
from shared.messages import PlaceOrder, SessionEvent

from arena import Exchange
from arena.exchange import _AdaptedExchangeServer
from team.risk import VenueGuard, VenueLimits

logger = logging.getLogger(__name__)

FLOW_LOG_EVERY = 500        # log the venue's flow share this often, in trades


class MyExchange(Exchange):
    """Your venue policy."""

    # Published fee schedule, bps of notional: nets the venue 3 bps a trade.
    # The hosted venue takes its schedule from the portal (MY TEAM → MY VENUE),
    # so keep these two in step with what /api/venues shows for STT_11.
    taker_bps = 14      # None → class default
    rebate_bps = 11

    def __init__(self, limits: VenueLimits | None = None) -> None:
        self._volume: Counter[str] = Counter()
        self._trades = 0
        self.guard = VenueGuard(limits)

    def accept_order(self, order: PlaceOrder, portfolio: Any) -> tuple[bool, str]:
        """Rate limit, fat-finger checks and the halt policy (VenueGuard)."""
        team = getattr(portfolio, "team_id", None) or order.team_id
        ok, why = self.guard.accept(team, order.symbol, order.side, order.order_type,
                                    order.price, order.quantity,
                                    exempt=_config.is_house_bot(team))
        if not ok:
            logger.info("VENUE REJECT %s %s %d %s @ %s: %s", team, order.side,
                        order.quantity, order.symbol, order.price, why)
        return ok, why

    def on_event(self, event: str, data: dict) -> None:
        """Session events the engine broadcasts: halts and resumes."""
        self.guard.on_event(event, data)
        if event in ("TRADING_HALT", "TRADING_RESUMED", "MARKET_HALT", "MARKET_RESUMED"):
            logger.info("VENUE %s %s — policy updated", event, data.get("symbol") or "MARKET")

    def on_trade(self, symbol: str, price: float, quantity: int, buyer_id: str,
                 seller_id: str) -> None:
        """Track who the venue's notional comes from, to judge the fee schedule."""
        self.guard.on_trade(symbol, price)
        notional = price * quantity
        self._volume[buyer_id] += notional
        self._volume[seller_id] += notional
        self._trades += 1
        if self._trades % FLOW_LOG_EVERY == 0:
            logger.info("Flow after %d trades: %s | venue rejects %s", self._trades,
                        ", ".join(f"{who} ${n:,.0f}" for who, n in self._volume.most_common(5)),
                        self.guard.rejected or "none")

    async def _serve(self) -> None:
        """Arena's _serve, with a server that also forwards session events."""
        server = _PolicyServer(self)
        async with websockets.serve(server.handle_client, *bind_target()):
            _print_startup(server)
            try:
                await asyncio.gather(
                    server._book_snapshot_loop(),
                    server._leaderboard_loop(),
                    server._price_tick_loop(),
                    server._teacher_cli(),
                )
            except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
                pass
        logger.info("Venue stopped.")


class _PolicyServer(_AdaptedExchangeServer):
    """The arena adapter forwards trades only; the halt policy needs events too."""

    async def _broadcast(self, message: Any, **kwargs: Any) -> None:
        await super()._broadcast(message, **kwargs)
        if isinstance(message, SessionEvent):
            try:
                self._policy.on_event(message.event, message.data or {})
            except Exception:
                logger.exception("on_event raised — ignored")


if __name__ == "__main__":
    MyExchange().run()
