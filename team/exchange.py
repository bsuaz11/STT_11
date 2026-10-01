"""
STT_11 — your exchange venue (port 8767).

The matching engine, settlement, and market data are provided. You set
policy: the fee schedule you publish, and the orders you accept.

Run:
    EXCHANGE_PORT=8767 python -m team.exchange
"""

from __future__ import annotations

from arena import Exchange


class MyExchange(Exchange):
    """Your venue policy."""

    # TODO: pick your published fee schedule (bps of notional).
    # Undercut the arena's 15/10 to attract flow, or charge premium.
    # Every trader sees it on the dashboard (STATS → venue fee schedules).
    taker_bps = None      # None → class default
    rebate_bps = None

    # Optional extra hooks:
    #   def accept_order(self, order, portfolio): return True, ""
    #   def on_trade(self, symbol, price, quantity, buyer_id, seller_id): ...


if __name__ == "__main__":
    MyExchange().run()
