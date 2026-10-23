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
import json
import logging
import re
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
# per TEAM — the trader seats share it — with a burst allowance of 3× that
# (exchange/config.py ORDER_QUOTA_PER_TICK, ORDER_QUOTA_BURST_MULTIPLE; weeks
# 1-6 use the default, week 7+ set "order_quota" in the scenario). Requoting
# one symbol cancels its old ladder and posts a new one (up to 12 messages
# with 3 levels), so spend at most this many messages in any rolling second.
# The traders send at most ~2/s each (one IOC per 0.5 s tick), so 14 + 2 + 2
# stays under 20. If the scenario sets a quota, or a RATE_LIMITED reject names
# one, the budget drops to that quota minus TRADER_RESERVE.
MESSAGE_BUDGET = 14
TRADER_RESERVE = 5

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
# Never quote a name through a scheduled print: a ramp moves 26-78 bps a
# tick, far past our width, so every resting quote on the wrong side gets
# hit. The full calendar is broadcast at the open, so instead of skipping a
# named symbol all session we step aside only around each event, timed in
# exchange ticks (1 s each):
#   earnings   EVENT_PRE_TICKS before: quote only the side that shrinks
#              inventory, at the centre; EVENT_FLATTEN_TICKS before: pull
#              quotes and IOC out of what is left; back EVENT_POST_TICKS after
#              (ramp 15 + settle ~8 + margin)
#   econ print every symbol: pull quotes ECON_PRE_TICKS before, back
#              ECON_POST_TICKS after (no flatten: the move is small and its
#              sign a coin flip, so flattening only buys variance with fees)
#   dividend   pull the symbol's quotes across the ex-dividend price drop
SKIP_SYMBOLS: set[str] = set()      # never quoted (keep in sync with team/trader.py)
SHOCK_PAUSE_SEC = 30                # pull quotes this long after an unscheduled SHOCK
EXCHANGE_TICK_SEC = 1.0
EVENT_PRE_TICKS = 20
EVENT_FLATTEN_TICKS = 8             # room for a retry after the cancel grace (3 s)
FLATTEN_RETRY_SEC = 1.5
EVENT_POST_TICKS = 35
ECON_PRE_TICKS = 3
ECON_POST_TICKS = 25
DIVIDEND_PRE_TICKS = 2
DIVIDEND_POST_TICKS = 3
FLATTEN_SLIP_BPS = 30               # IOC this far through the centre so it fills

# ── New listings ─────────────────────────────────────────────────────────
# A listing opens at the offer and walks to a hidden value: one-way flow
# that runs a market maker over. Quote it wider and smaller at first.
LISTING_WIDE_SEC = 300
LISTING_SPREAD_MULT = 3.0
LISTING_MAX_SHARES = 50             # long and short, instead of the BROKER_MAX_ caps

# ── Volatility ───────────────────────────────────────────────────────────
# Width = max(the floor above, VOL_MULT × the std dev of the last VOL_WINDOW
# mark-to-mark returns), so a half-spread covers ~1.5 moves of the mark. In a
# calm market the floor wins and nothing changes.
VOL_WINDOW = 30
VOL_MULT = 3.0
MAX_SPREAD_BPS = 300                # never quote wider than this, all widening included

# ── Toxic flow ───────────────────────────────────────────────────────────
# Mark out every passive fill MARKOUT_SEC later: how far the mark moved
# against us, in bps. A counterparty whose last TOXIC_MIN_FILLS+ fills average
# worse than TOXIC_MARKOUT_BPS is picking us off; when it trades a symbol we
# widen that symbol TOXIC_SPREAD_MULT× for TOXIC_WIDEN_SEC. (The SDK's
# toxic() hook is never called by the plumbing, so this lives here.)
MARKOUT_SEC = 5.0
TOXIC_MIN_FILLS = 6
TOXIC_MARKOUT_BPS = -15.0
TOXIC_SPREAD_MULT = 2.0
TOXIC_WIDEN_SEC = 30
OWN_TEAM_PREFIX = "stt_11_"         # our own seats are never toxic

# ── Reject safety ────────────────────────────────────────────────────────
# Every PlaceOrder passes _guard() first, which sizes it against the WORST
# case: every order we have sent and not seen die is assumed to fill, even if
# its ack or fill hasn't reached us yet. Under that assumption the venue's
# own checks cannot fail:
#   SHORTS_LOCKED     sell ≤ position − every other sell we may still have out
#                     (+ BROKER_MAX_SHORT once the scenario allows shorts)
#   SSR_RESTRICTED    no new shorts in a symbol under the short-sale rule
#   BORROW_UNAVAILABLE no new shorts unless leaderboard short interest leaves
#                     BORROW_HEADROOM under the locate cap; a reject pauses
#                     shorting that symbol for BORROW_PAUSE_SEC
#   POSITION_LIMIT    position + open bids + this bid ≤ BROKER_MAX_SHARES and
#                     position − open asks − this ask ≥ −BROKER_MAX_SHORT;
#                     the limit is per TEAM, so team/trader.py sizes its own
#                     trades around both numbers
#   INSUFFICIENT_CASH open bid notional + this bid ≤ CASH_USE × cash
#   UNKNOWN_SYMBOL    only symbols the venue has sent a book for
#   SESSION_CLOSED    no orders in QUIET_WINDOWS around the scheduled closes
BROKER_MAX_SHARES = 150             # keep in sync with team/trader.py
BROKER_MAX_SHORT = 150              # keep in sync with team/trader.py
SHORT_LOCATE_CAP = 2000             # replaced at SESSION_OPEN from the scenario
BORROW_HEADROOM = 300
BORROW_PAUSE_SEC = 120
SHORT_REJECTS = ("SHORTS_LOCKED", "SSR_RESTRICTED", "BORROW_UNAVAILABLE")
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


def short_interest(leaderboard) -> dict[str, int]:
    """Shares short per symbol across every seat on the leaderboard — what
    the venue's borrow cap is measured against."""
    out: dict[str, int] = {}
    for entry in list(leaderboard.traders or []) + list(leaderboard.brokers or []):
        for sym, qty in (entry.get("positions") or {}).items():
            if qty < 0:
                out[sym] = out.get(sym, 0) - int(qty)
    return out


class _RejectTap:
    """Wraps the websocket so the bot sees rejects (ErrorMsg), which the
    stock bot only logs."""

    def __init__(self, ws, bot):
        self._ws, self._bot = ws, bot

    def __getattr__(self, name):
        return getattr(self._ws, name)

    def __aiter__(self):
        return self._tap()

    async def _tap(self):
        async for raw in self._ws:
            if '"error"' in raw:
                try:
                    msg = json.loads(raw)
                    if msg.get("type") == "error":
                        self._bot._on_reject(msg.get("code"), msg.get("message"))
                except Exception:
                    logger.exception("ignoring unreadable error")
            yield raw


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
        self._shorts_ok = False                 # set from the scenario at SESSION_OPEN
        self._locate_cap = SHORT_LOCATE_CAP
        self._ssr: set[str] = set()             # symbols under the short-sale rule
        self._no_short_until: dict[str, float] = {}
        self._short_interest: dict[str, int] = {}
        self._budget = MESSAGE_BUDGET
        self._tick_ref: tuple[int, float] | None = None   # (exchange tick, monotonic) last seen
        self._events: dict[tuple, dict] = {}    # (kind, symbol, tick) -> public event
        self._flatten_retry: dict[str, float] = {}   # symbol -> next flatten attempt
        self._markouts_due: deque = deque()     # passive fills awaiting their markout

    # ── Event schedule ──────────────────────────────────────────────────
    def _note_tick(self, tick) -> None:
        if isinstance(tick, (int, float)):
            self._tick_ref = (int(tick), time.monotonic())

    def _tick(self) -> float | None:
        if self._tick_ref is None:
            return None
        tick, at = self._tick_ref
        return tick + (time.monotonic() - at) / EXCHANGE_TICK_SEC

    def _event_phase(self, sym):
        """("out" | "reduce" | None, the earnings event to flatten for)."""
        now = self._tick()
        if now is None:
            return None, None
        phase, flatten = None, None
        for (kind, esym, etick), ev in self._events.items():
            d = etick - now                       # ticks until the event
            if kind == "earnings" and esym == sym:
                if -EVENT_POST_TICKS < d <= EVENT_FLATTEN_TICKS:
                    phase, flatten = "out", ev
                elif d <= EVENT_PRE_TICKS and d > EVENT_FLATTEN_TICKS and phase is None:
                    phase = "reduce"
            elif ev.get("market_wide") and kind != "dividend":
                if -ECON_POST_TICKS < d <= ECON_PRE_TICKS:
                    phase = "out"
            elif kind == "dividend" and esym == sym:
                if -DIVIDEND_POST_TICKS < d <= DIVIDEND_PRE_TICKS:
                    phase = "out"
        return phase, flatten

    def _caps(self, sym):
        """(most shares long, most shares short) this symbol may reach."""
        if time.monotonic() < self._owner._listing_until.get(sym, 0.0):
            return LISTING_MAX_SHARES, LISTING_MAX_SHARES
        return BROKER_MAX_SHARES, BROKER_MAX_SHORT

    async def _flatten_for_events(self) -> None:
        """IOC out of inventory in a symbol whose earnings print is imminent."""
        now = time.monotonic()
        for sym in list(self._quote_symbols):
            phase, ev = self._event_phase(sym)
            pos = self.state.positions.get(sym, 0)
            if ev is None or not pos or ev["tick"] < (self._tick() or 0):
                continue                          # nothing to do, or the print already hit
            centre = self.quote_centre(sym)
            # Retry until flat: the guard may shrink the first IOC while the
            # quotes just cancelled are still counted as open.
            if not centre or now < self._flatten_retry.get(sym, 0.0):
                continue
            self._flatten_retry[sym] = now + FLATTEN_RETRY_SEC
            await self.cancel_quotes(sym)
            side = "sell" if pos > 0 else "buy"
            slip = FLATTEN_SLIP_BPS / 1e4
            px = round(centre * (1 - slip if side == "sell" else 1 + slip), 2)
            logger.info("Flatten before %s print: %s %d %s @ %.2f",
                        sym, side, abs(pos), sym, px)
            await self._send(PlaceOrder(
                team_id=_broker_config.TEAM_ID, symbol=sym, side=side,
                order_type="ioc", price=px, quantity=min(abs(pos), 500)))

    # ── Toxic flow ──────────────────────────────────────────────────────
    def _mark_out(self) -> None:
        """Score fills old enough to judge: + means the mark moved our way."""
        now = time.monotonic()
        while self._markouts_due and now - self._markouts_due[0][0] >= MARKOUT_SEC:
            _, cpty, sym, side, px = self._markouts_due.popleft()
            mark = self.state.exchange_prices.get(sym)
            if not mark or not px:
                continue
            bps = (mark / px - 1) * 1e4 * (1 if side == "buy" else -1)
            self._owner._markouts.setdefault(cpty, deque(maxlen=20)).append(bps)
            if self._owner.toxic(cpty):
                logger.info("Toxic: %s avg markout %.1f bps over %d fills", cpty,
                            sum(self._owner._markouts[cpty]) / len(self._owner._markouts[cpty]),
                            len(self._owner._markouts[cpty]))

    def _may_short(self, sym) -> bool:
        return (self._shorts_ok and sym not in self._ssr
                and time.monotonic() >= self._no_short_until.get(sym, 0.0)
                and self._short_interest.get(sym, 0) + BROKER_MAX_SHORT + BORROW_HEADROOM
                <= self._locate_cap)

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
        max_long, max_short = self._caps(sym)
        if self._event_phase(sym)[0] and msg.order_type != "ioc":
            max_long = max_short = 0              # print coming: only shrink inventory
        if msg.side == "sell":
            short_room = max_short if self._may_short(sym) else 0
            qty = min(qty, pos + short_room - self._open(sym, "sell"))
        else:
            qty = min(qty, max_long - pos - self._open(sym, "buy"))
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
                "rem": msg.quantity, "oid": None,
                # An IOC is dead on arrival: count it only while its fills land.
                "dead": time.monotonic() if msg.order_type == "ioc" else None,
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
        cpty = msg.seller_id if side == "buy" else msg.buyer_id
        if msg.maker_id == me and not cpty.startswith(OWN_TEAM_PREFIX):
            self._markouts_due.append((time.monotonic(), cpty, msg.symbol, side, msg.price))
            if self._owner.toxic(cpty) and time.monotonic() >= self._owner._toxic_until.get(msg.symbol, 0):
                self._owner._toxic_until[msg.symbol] = time.monotonic() + TOXIC_WIDEN_SEC
                self.state.last_quote_prices.pop(msg.symbol, None)   # requote wider now
                logger.info("Widening %s for %ds: filled by toxic %s",
                            msg.symbol, TOXIC_WIDEN_SEC, cpty)
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

    async def connect_to_exchange(self) -> None:
        await super().connect_to_exchange()
        self._ws = _RejectTap(self._ws, self)

    def _on_reject(self, code, message) -> None:
        if code == "RATE_LIMITED":
            m = re.search(r"send (\d+)", message or "")
            if m:
                self._budget = min(self._budget, max(2, int(m.group(1)) - TRADER_RESERVE))
            logger.warning("RATE_LIMITED: %s — budget now %d/s", message, self._budget)
            return
        if code not in SHORT_REJECTS:
            return
        sym = next((s for s in self._quote_symbols
                    if re.search(rf"\b{re.escape(s)}\b", message or "")), None)
        logger.warning("Short rejected [%s] %s — no new shorts there for now", code, sym or "(all)")
        if code == "SHORTS_LOCKED":
            self._shorts_ok = False
        elif code == "SSR_RESTRICTED" and sym:
            self._ssr.add(sym)
        else:
            until = time.monotonic() + BORROW_PAUSE_SEC
            for s in [sym] if sym else self._quote_symbols:
                self._no_short_until[s] = until

    def _on_leaderboard(self, msg) -> None:
        super()._on_leaderboard(msg)
        self._short_interest = short_interest(msg)
        self._note_tick(getattr(msg, "tick", None))
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
        self._note_tick(data.get("tick"))
        if msg.event == "SESSION_OPEN":
            sc = data.get("scenario") or {}
            self._shorts_ok = bool((sc.get("flags") or {}).get("shorts_allowed", False))
            self._locate_cap = int((sc.get("config") or {}).get("SHORT_LOCATE_CAP",
                                                               self._locate_cap))
            quota = sc.get("order_quota")
            self._budget = (min(MESSAGE_BUDGET, max(2, int(quota) - TRADER_RESERVE))
                            if quota else MESSAGE_BUDGET)
            if not data.get("late_join"):       # SSR is sticky until the next open
                self._ssr.clear()
                self._events.clear()
            logger.info("Session rules: shorts=%s (up to %d per symbol), locate cap %d, "
                        "venue quota %s/tick → broker budget %d/s",
                        self._shorts_ok, BROKER_MAX_SHORT, self._locate_cap,
                        quota or "default (20)", self._budget)
        elif msg.event == "SSR_ON" and data.get("symbol"):
            self._ssr.add(data["symbol"])
        elif msg.event == "CALENDAR":
            for ev in data.get("events", []) + data.get("imminent", []):
                key = (ev.get("kind"), ev.get("symbol"), ev.get("tick"))
                if isinstance(key[2], int) and key not in self._events:
                    self._events[key] = ev
                    logger.info("Scheduled: %s %s at tick %d — stepping aside around it",
                                key[0], key[1] or "MARKET", key[2])
        elif msg.event == "IPO_LISTED" and data.get("symbol"):
            self._owner._listing_until[data["symbol"]] = time.monotonic() + LISTING_WIDE_SEC
            logger.info("New listing %s: %.0f× width, ±%d shares for %ds", data["symbol"],
                        LISTING_SPREAD_MULT, LISTING_MAX_SHARES, LISTING_WIDE_SEC)
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
        return self._budget - len(self._sent)

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
        return (sym in self._skip or time.monotonic() < self._paused.get(sym, 0)
                or self._event_phase(sym)[0] == "out")

    async def requote_loop(self) -> None:
        while True:
            await asyncio.sleep(_broker_config.REQUOTE_INTERVAL_SEC)
            if not self._session_open:
                continue
            self._mark_out()
            # Shrink-only quoting before a print: skew the reducing side to the centre.
            self._owner._reducing = {s for s in self._quote_symbols
                                     if self._event_phase(s)[0] == "reduce"}
            if not _in_quiet_window():
                await self._flatten_for_events()
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
        # Written by BudgetedBrokerBot (shared across venues):
        self._listing_until: dict[str, float] = {}   # new listing -> wide-quote end
        self._toxic_until: dict[str, float] = {}     # symbol -> toxic-widening end
        self._markouts: dict[str, deque] = {}        # counterparty -> recent markouts (bps)
        self._reducing: set[str] = set()             # symbols with a print coming

    def spread(self, symbol, price, history):
        # `history` is the recent reference prices, oldest first.
        # Return the dollar width, or None for the fixed default.
        if not price:
            return None
        width = max(min(0.30, price * 25 / 1e4),  # the default width
                    price * MIN_SPREAD_BPS / 1e4)
        h = history[-(VOL_WINDOW + 1):]
        rets = [b / a - 1 for a, b in zip(h, h[1:]) if a > 0]
        if len(rets) >= VOL_WINDOW // 2:
            mean = sum(rets) / len(rets)
            vol = (sum((r - mean) ** 2 for r in rets) / len(rets)) ** 0.5
            width = max(width, VOL_MULT * vol * price)
        now = time.monotonic()
        if now < self._listing_until.get(symbol, 0.0):
            width *= LISTING_SPREAD_MULT
        if now < self._toxic_until.get(symbol, 0.0):
            width *= TOXIC_SPREAD_MULT
        width = min(width, price * MAX_SPREAD_BPS / 1e4)
        self._half_spread[symbol] = width / 2
        return width

    def skew(self, symbol, inventory):
        # Shift both quotes against our inventory: long → quote lower so our
        # offer gets lifted first. At MAX_INVENTORY the shift is a full
        # half-spread, which puts the reducing side at the mid. With a print
        # coming, any inventory gets the full shift.
        half = self._half_spread.get(symbol)
        if not half or not inventory:
            return None
        if symbol in self._reducing:
            return -half if inventory > 0 else half
        ratio = max(-1.0, min(1.0, inventory / MAX_INVENTORY))
        return -ratio * half

    def toxic(self, trader_id):
        marks = self._markouts.get(trader_id)
        return (marks is not None and len(marks) >= TOXIC_MIN_FILLS
                and sum(marks) / len(marks) < TOXIC_MARKOUT_BPS)

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
