"""
STT_11 — your exchange venue (port 8767).

The matching engine, settlement, and market data are provided. You set
policy: the fee schedule you publish, and the orders you accept.

Run:
    EXCHANGE_PORT=8767 python -m team.exchange
"""

from __future__ import annotations

import logging
from collections import Counter

from arena import Exchange

logger = logging.getLogger(__name__)

FLOW_LOG_EVERY = 500        # log the venue's flow share this often, in trades


class MyExchange(Exchange):
    """Your venue policy."""

    # Published fee schedule, bps of notional: nets the venue 3 bps a trade.
    # The hosted venue takes its schedule from the portal (MY TEAM → MY VENUE),
    # so keep these two in step with what /api/venues shows for STT_11.
    taker_bps = 14      # None → class default
    rebate_bps = 11

    def __init__(self) -> None:
        self._volume: Counter[str] = Counter()
        self._trades = 0

    def on_trade(self, symbol, price, quantity, buyer_id, seller_id):
        """Track who the venue's notional comes from, to judge the fee schedule."""
        notional = price * quantity
        self._volume[buyer_id] += notional
        self._volume[seller_id] += notional
        self._trades += 1
        if self._trades % FLOW_LOG_EVERY == 0:
            logger.info("Flow after %d trades: %s", self._trades,
                        ", ".join(f"{who} ${n:,.0f}" for who, n in self._volume.most_common(5)))


if __name__ == "__main__":
    MyExchange().run()
