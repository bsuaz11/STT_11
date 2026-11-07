"""
Phase2/event_gap.py — the post-print fair-value gap signal.

Hypothesis
----------
A CALENDAR_EVENT publishes a signed move, so the moment it arrives everyone
knows the new fair value: ``pre_mark × (1 + pct)``. Liquidity does not reprice
at that moment. Some resting orders are still priced off the old value, and
the venue walks its mark over a capped ramp. Any order still on the wrong side
of the new fair value by more than the round-trip cost is stale and worth
taking; the price converges to fair value within the ramp and its settle.

The rule
--------
Entry   good news: buy while ``ask <= fair × (1 - edge)``.
        bad news: sell while ``bid >= fair × (1 + edge)``.
        ``edge`` = 2 × taker fee + ``buffer_bps``. The IOC's limit IS that
        price, so an order can never pay through the gate.
Exit    when the touch reaches ``pre_mark × (1 + pct × target_mult)``, or at
        ``max_hold_ticks`` after the print, whichever comes first.

This module is pure: no sockets, no wall clock, no engine imports. Time is the
exchange tick handed in by the caller, so the live bot, the simulator and the
tape replay all run this exact code.

Look-ahead rule: ``on_print`` takes the mark from BEFORE the print and the
decision methods take only the touch at the current tick. Nothing here can see
a later tick.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from math import ceil, floor

MAX_ORDER_SHARES = 500      # venue rejects anything larger (INVALID_QUANTITY)


@dataclass(frozen=True)
class GapParams:
    """The signal's free parameters. These defaults are the values frozen by
    Phase2/backtest.py on training seeds; the live bot runs them unchanged."""

    buffer_bps: float = 0.0       # edge required on top of the round-trip fee
    target_mult: float = 1.0      # exit at this multiple of the print (1.0 = settle)
    max_entry_ticks: int = 15     # stop opening this long after the print
    max_hold_ticks: int = 30      # time stop: ramp + settle + margin
    exit_slip_bps: float = 30.0   # how far through the touch an exit IOC reaches


@dataclass(frozen=True)
class Order:
    """An IOC the signal wants sent."""

    symbol: str
    side: str       # "buy" | "sell"
    quantity: int
    price: float
    reason: str     # "entry" | "target" | "time_stop"


@dataclass
class Plan:
    """One print being traded in one symbol."""

    symbol: str
    pct: float
    pre_mark: float
    t0: int
    base: int                       # position when the print arrived
    fair: float
    target: float
    bought: int = 0
    sold: int = 0
    gross: float = 0.0              # cash in minus cash out, before fees
    fees: float = 0.0

    def inventory(self, position: int) -> int:
        """Shares held because of this plan (negative when short)."""
        return position - self.base


@dataclass(frozen=True)
class PlanResult:
    """A finished plan: the row the backtest and the live log both report."""

    symbol: str
    pct: float
    t0: int
    t1: int
    bought: int
    sold: int
    pnl: float      # net of fees; only meaningful once bought == sold
    fees: float

    @property
    def traded(self) -> bool:
        """True if any of the plan's orders filled."""
        return self.bought > 0 or self.sold > 0


@dataclass
class EventGapSignal:
    """Tracks open plans and turns the touch into at most one order per call."""

    params: GapParams = field(default_factory=GapParams)
    taker_fee: float = 0.004        # fraction of notional; live value via FEE_SCHEDULE
    plans: dict[str, Plan] = field(default_factory=dict)
    closed: list[PlanResult] = field(default_factory=list)

    # ── Inputs ──────────────────────────────────────────────────────────
    def set_fee(self, taker_fee: float) -> None:
        """Adopt the venue's published taker fee."""
        self.taker_fee = float(taker_fee)

    @property
    def edge(self) -> float:
        """Required distance from fair value, as a fraction."""
        return 2 * self.taker_fee + self.params.buffer_bps / 1e4

    def on_print(self, symbol: str, pct: float, pre_mark: float, tick: int,
                 position: int) -> Plan | None:
        """Open a plan. `pre_mark` must be the mark from before the print.

        A second print in a symbol still being traded keeps the first plan's
        base position, so the shares it holds are still exited.
        """
        if not pre_mark or pre_mark <= 0 or not pct:
            return None
        old = self.plans.get(symbol)
        plan = Plan(symbol=symbol, pct=pct, pre_mark=pre_mark, t0=tick,
                    base=old.base if old else position,
                    fair=pre_mark * (1 + pct),
                    target=pre_mark * (1 + pct * self.params.target_mult))
        if old:
            plan.bought, plan.sold = old.bought, old.sold
            plan.gross, plan.fees = old.gross, old.fees
        self.plans[symbol] = plan
        return plan

    def on_fill(self, symbol: str, side: str, quantity: int, price: float) -> None:
        """Charge a fill to the symbol's open plan, if there is one."""
        plan = self.plans.get(symbol)
        if plan is None:
            return
        notional = quantity * price
        plan.fees += notional * self.taker_fee
        if side == "buy":
            plan.bought += quantity
            plan.gross -= notional
        else:
            plan.sold += quantity
            plan.gross += notional

    # ── The decision ────────────────────────────────────────────────────
    def decide(self, symbol: str, tick: int, bid: float | None, ask: float | None,
               position: int, long_room: int, short_room: int) -> Order | None:
        """What to do in `symbol` at `tick`, given the touch and the room left.

        `long_room` / `short_room` are shares the caller's risk limits allow in
        each direction from `position`. Returns None to do nothing.
        """
        plan = self.plans.get(symbol)
        if plan is None:
            return None
        age = tick - plan.t0
        held = plan.inventory(position)
        long_side = plan.pct > 0

        if held and (age >= self.params.max_hold_ticks
                     or self._at_target(plan, bid, ask)):
            why = "time_stop" if age >= self.params.max_hold_ticks else "target"
            order = self._exit(plan, held, bid, ask, why)
            if order is not None:
                return order

        if age < self.params.max_entry_ticks:
            order = self._entry(plan, bid, ask, long_room, short_room, long_side)
            if order is not None:
                return order

        if not held and age >= self.params.max_entry_ticks:
            self._close(symbol, tick)
        return None

    def _entry(self, plan: Plan, bid: float | None, ask: float | None, long_room: int,
               short_room: int, long_side: bool) -> Order | None:
        if long_side:
            limit = floor(plan.fair * (1 - self.edge) * 100) / 100
            if ask is None or ask > limit or long_room <= 0:
                return None
            return Order(plan.symbol, "buy", min(long_room, MAX_ORDER_SHARES), limit, "entry")
        limit = ceil(plan.fair * (1 + self.edge) * 100) / 100
        if bid is None or bid < limit or short_room <= 0:
            return None
        return Order(plan.symbol, "sell", min(short_room, MAX_ORDER_SHARES), limit, "entry")

    def _at_target(self, plan: Plan, bid: float | None, ask: float | None) -> bool:
        if plan.pct > 0:
            return bid is not None and bid >= plan.target
        return ask is not None and ask <= plan.target

    def _exit(self, plan: Plan, held: int, bid: float | None, ask: float | None,
              reason: str) -> Order | None:
        slip = self.params.exit_slip_bps / 1e4
        qty = min(abs(held), MAX_ORDER_SHARES)
        if held > 0 and bid is not None:
            return Order(plan.symbol, "sell", qty, round(bid * (1 - slip), 2), reason)
        if held < 0 and ask is not None:
            return Order(plan.symbol, "buy", qty, round(ask * (1 + slip), 2), reason)
        return None                     # that side of the book is empty: wait

    def _close(self, symbol: str, tick: int) -> None:
        plan = self.plans.pop(symbol)
        self.closed.append(PlanResult(symbol=symbol, pct=plan.pct, t0=plan.t0, t1=tick,
                                      bought=plan.bought, sold=plan.sold,
                                      pnl=plan.gross - plan.fees, fees=plan.fees))
