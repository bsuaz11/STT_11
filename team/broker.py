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
from datetime import datetime
from zoneinfo import ZoneInfo

import broker.config as _broker_config
from shared.messages import CancelOrder, PlaceOrder

from arena import Broker
from arena.broker import _AdaptedBrokerBot

logger = logging.getLogger(__name__)
_CHICAGO = ZoneInfo("America/Chicago")

# The venue allows 20 order/cancel messages per exchange tick (1 s), metered
# per TEAM — the trader seat shares it. Requoting one symbol cancels its old
# ladder and posts a new one (up to 12 messages with 3 levels), so spend at
# most this many messages in any rolling second, leaving the trader headroom.
MESSAGE_BUDGET = 14

# Pricing. In the headless simulator at live fees (30 bps taker, 5 bps
# rebate), a 30 bps minimum width plus inventory skew beat the default
# quoting on every one of 5 seeds: wider quotes get picked off less, and the
# skew sheds inventory before a trend turns it into a loss. Replayed on 30
# minutes of the real public order book, reaching full skew at 100 shares
# instead of 300 did better (+$1,222 vs +$961) and lost on fewer symbols;
# 20 bps looked better on the replay but was the worst in the simulator.
MIN_SPREAD_BPS = 30   # never quote narrower than this (default width is ≤25 bps)
MAX_INVENTORY = 100   # shares at which the skew reaches a full half-spread

# ── Event safety ─────────────────────────────────────────────────────────
# Never quote a name with a scheduled print: a ramp moves 26-78 bps a tick,
# far past our width, so every resting quote on the wrong side gets hit.
# Symbols named in CALENDAR announcements are added to this at runtime.
SKIP_SYMBOLS = {"NVDA", "TSLA"}     # week 3 earnings (team/trader.py trades them)
SHOCK_PAUSE_SEC = 30                # pull quotes this long after a teacher SHOCK

# ── Reject safety ────────────────────────────────────────────────────────
# Every PlaceOrder passes _guard() first, which sizes it against the WORST
# case: every order we have sent and not seen die is assumed to fill, even if
# its ack or fill hasn't reached us yet. Under that assumption the venue's
# own checks cannot fail:
#   SHORTS_LOCKED     sell ≤ position − every other sell we may still have out
#   POSITION_LIMIT    position + open bids + this bid ≤ BROKER_MAX_SHARES;
#                     the limit is per TEAM, so team/trader.py keeps its own
#                     buys at least BROKER_MAX_SHARES under the cap
#   INSUFFICIENT_CASH open bid notional + this bid ≤ CASH_USE × cash
#   UNKNOWN_SYMBOL    only symbols the venue has sent a book for
#   SESSION_CLOSED    no orders in QUIET_WINDOWS around the scheduled closes
BROKER_MAX_SHARES = 150             # keep in sync with team/trader.py
CASH_USE = 0.85                     # bid with at most this share of cash
CANCEL_GRACE_SEC = 3.0              # a cancelled order may still report fills this long
ACK_TIMEOUT_SEC = 5.0               # an order never acked was never live
# Chicago wall-clock windows with no new orders: the block's scheduled
# closes (open play → week 3 at 15:30, week 3 → open play at 16:30). An order
# in flight when SESSION_CLOSED lands is rejected.
QUIET_WINDOWS = (("15:29:45", "15:30:20"), ("16:29:45", "16:30:20"))


def _in_quiet_window(now: datetime | None = None) -> bool:
    t = (now or datetime.now(_CHICAGO)).strftime("%H:%M:%S")
    return any(lo <= t <= hi for lo, hi in QUIET_WINDOWS)


class BudgetedBrokerBot(_AdaptedBrokerBot):
    """Standard quoting with a message budget, event avoidance and a
    worst-case order ledger that keeps every order inside the venue's checks.

    Symbols that need a requote are served most-urgent first (no quote at
    all, then biggest price move); whatever doesn't fit in this pass's budget
    waits for the next pass, 0.5s later.
    """

    def __init__(self, owner, exchange_url=None):
        super().__init__(owner, exchange_url)
        self._sent = deque()                    # send times, for the quota
        self._orders: dict[int, dict] = {}      # our orders that may still fill
        self._by_oid: dict[str, int] = {}       # venue order id -> ledger key
        self._filled_oids: set[str] = set()     # certainly filled: don't cancel them
        self._next_key = 0
        self._cash_est: float | None = None     # last PortfolioUpdate ± fills since
        self._skip = set(SKIP_SYMBOLS)
        self._paused: dict[str, float] = {}     # symbol -> pause end (monotonic)
        self.blocked = 0                        # orders _guard() dropped or shrank
        self._warned_at = 0.0

    # ── The ledger ──────────────────────────────────────────────────────
    def _live(self):
        """Orders that may still fill. Drops filled orders once their ack is
        matched (kept until then so the ack can't land on the wrong order),
        cancelled orders past their grace period, and orders never acked."""
        now = time.monotonic()
        for k, o in list(self._orders.items()):
            if ((o["rem"] <= 0 and o["oid"] is not None)   # filled; ack matched
                    or (o["dead"] is not None and now - o["dead"] > CANCEL_GRACE_SEC)
                    or (o["oid"] is None and now - o["sent"] > ACK_TIMEOUT_SEC)):
                self._orders.pop(k)
                if o["oid"]:
                    self._by_oid.pop(o["oid"], None)
        return self._orders.values()

    def _open(self, symbol, side):
        return sum(o["rem"] for o in self._live()
                   if o["sym"] == symbol and o["side"] == side)

    def _open_bid_cash(self):
        return sum(o["rem"] * o["px"] for o in self._live() if o["side"] == "buy")

    def _guard(self, msg: PlaceOrder) -> PlaceOrder | None:
        """Shrink or drop an order so no venue check can reject it."""
        sym, qty = msg.symbol, msg.quantity
        if (not self._session_open or _in_quiet_window()
                or sym not in self.state.exchange_prices
                or sym in self._skip or time.monotonic() < self._paused.get(sym, 0)):
            return None
        pos = self.state.positions.get(sym, 0)
        if msg.side == "sell":
            qty = min(qty, pos - self._open(sym, "sell"))
        else:
            qty = min(qty, BROKER_MAX_SHARES - pos - self._open(sym, "buy"))
            if self._cash_est is None:          # no portfolio yet
                if time.monotonic() - self._warned_at > 30:
                    self._warned_at = time.monotonic()
                    logger.warning("No portfolio from the venue yet — not bidding "
                                   "until a PortfolioUpdate or leaderboard arrives")
                return None
            per_share = msg.price * 1.01        # price + fee, with room
            free = CASH_USE * self._cash_est - self._open_bid_cash()
            qty = min(qty, int(free // per_share)) if per_share > 0 else 0
        qty = min(qty, 500)                     # venue max per order
        if qty != msg.quantity:
            self.blocked += 1
        if qty <= 0:
            return None
        return msg if qty == msg.quantity else msg.model_copy(update={"quantity": qty})

    async def _send(self, msg):
        if isinstance(msg, PlaceOrder):
            msg = self._guard(msg)
            if msg is None:
                return
            self._orders[self._next_key] = {
                "sym": msg.symbol, "side": msg.side, "px": round(msg.price, 2),
                "rem": msg.quantity, "oid": None, "dead": None,
                "sent": time.monotonic()}
            self._next_key += 1
        elif isinstance(msg, CancelOrder):
            if msg.order_id in self._filled_oids:
                # Already filled: cancelling it only earns an ORDER_NOT_FOUND.
                self._filled_oids.discard(msg.order_id)
                return
            k = self._by_oid.get(msg.order_id)
            if k is not None and k in self._orders:
                self._orders[k]["dead"] = time.monotonic()
        self._sent.append(time.monotonic())
        await super()._send(msg)

    def _on_order_ack(self, msg) -> None:
        super()._on_order_ack(msg)
        if msg.team_id != _broker_config.TEAM_ID:
            return
        unacked = [k for k, o in self._orders.items()
                   if o["oid"] is None and o["sym"] == msg.symbol
                   and o["side"] == msg.side]
        same_px = [k for k in unacked if self._orders[k]["px"] == round(msg.price, 2)]
        if same_px or unacked:
            k = (same_px or unacked)[0]
            self._orders[k]["oid"] = msg.order_id
            self._by_oid[msg.order_id] = k
            if self._orders[k]["rem"] <= 0 and self._orders[k].get("sure"):
                self._filled_oids.add(msg.order_id)

    def _on_trade(self, msg) -> None:
        super()._on_trade(msg)
        me = _broker_config.TEAM_ID
        side = "buy" if msg.buyer_id == me else "sell" if msg.seller_id == me else None
        if side is None:
            return
        if self._cash_est is not None:
            self._cash_est += msg.quantity * msg.price * (-1 if side == "buy" else 1)
        # A resting order fills at its own price. Charge the fill to a
        # cancelled order first: if we guess wrong we over-count what's open,
        # which only makes the guard stricter.
        left = msg.quantity
        match = sorted((o for o in self._live() if o["sym"] == msg.symbol
                        and o["side"] == side and o["px"] == round(msg.price, 2)),
                       key=lambda o: o["dead"] is None)
        for o in match:
            take = min(left, o["rem"])
            o["rem"] -= take
            left -= take
            if left <= 0:
                break
        # Skip the later cancel only when the fill could belong to just this
        # one order — a wrong guess would leave a live quote uncancelled.
        if len(match) == 1 and match[0]["rem"] <= 0:
            match[0]["sure"] = True
            if match[0]["oid"]:
                self._filled_oids.add(match[0]["oid"])

    def _on_portfolio(self, msg) -> None:
        super()._on_portfolio(msg)
        self._cash_est = msg.cash

    def _on_leaderboard(self, msg) -> None:
        super()._on_leaderboard(msg)
        # The venue sends a PortfolioUpdate only on a seat's FIRST connect of
        # a session, at the open and on fills — a reconnect gets none, which
        # left the guard with no cash and no shares (so no quotes at all).
        # The leaderboard (every ~2 s) carries the same numbers: seed from it.
        if self._cash_est is not None:
            return
        for entry in msg.brokers:
            if entry.get("team_id") == _broker_config.TEAM_ID:
                self._cash_est = float(entry.get("cash", 0.0))
                self.state.cash = self._cash_est
                self.state.positions = {k: int(v) for k, v in
                                        (entry.get("positions") or {}).items()}
                logger.info("Portfolio seeded from leaderboard: cash=%.2f positions=%s",
                            self._cash_est, self.state.positions)
                break

    async def flatten_all(self) -> None:
        # The stock bot sells everything at market on SESSION_CLOSED, after
        # the venue has closed: every one of those orders is rejected. Just
        # pull the quotes (cancels are never rejected for a closed session).
        await self.cancel_all_quotes()

    async def _on_session_event(self, msg) -> None:
        await super()._on_session_event(msg)
        data = msg.data or {}
        if msg.event == "CALENDAR":
            for ev in data.get("events", []) + data.get("imminent", []):
                if ev.get("symbol") and ev["symbol"] not in self._skip:
                    self._skip.add(ev["symbol"])
                    logger.info("Not quoting %s: scheduled %s", ev["symbol"], ev.get("kind"))
        elif msg.event in ("SHOCK", "CALENDAR_EVENT"):
            syms = data.get("affected") or ([data["symbol"]] if data.get("symbol") else [])
            for sym in syms:
                self._paused[sym] = time.monotonic() + SHOCK_PAUSE_SEC
            logger.info("Pulling quotes for %ss: %s", SHOCK_PAUSE_SEC, ", ".join(syms))

    # ── Requoting under the budget ──────────────────────────────────────
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

    def _blocked_symbol(self, sym: str) -> bool:
        return sym in self._skip or time.monotonic() < self._paused.get(sym, 0)

    async def requote_loop(self) -> None:
        while True:
            await asyncio.sleep(_broker_config.REQUOTE_INTERVAL_SEC)
            if not self._session_open:
                continue
            budget = self._room()

            # Pull whatever is resting in a symbol we've stopped quoting.
            for sym in list(self._quote_symbols):
                if self._blocked_symbol(sym) and sym in self.state.last_quote_prices:
                    cost = self._requote_cost(sym) - 2 * _broker_config.QUOTE_LEVELS
                    if cost <= budget:
                        budget -= cost
                        await self.cancel_quotes(sym)
            if _in_quiet_window():
                continue

            due = []
            for sym in list(self._quote_symbols):
                if self._blocked_symbol(sym):
                    continue
                centre = self.quote_centre(sym)
                if centre and self.needs_requote(sym, centre):
                    due.append((self._urgency(sym, centre), sym))
            due.sort(reverse=True)

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
