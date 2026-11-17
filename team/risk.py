"""
STT_11 — the risk layer (Phase 3). Every guard the seats share lives here.

The bots ask this module two questions:

    RiskManager.update(...)   once per tick: what must be closed right now?
    RiskManager.check(...)    before every order: how much of it may go out?

and tell it what happened (sends, rejects, fills, connects, resumes). Nothing
here sends an order. The trader and broker turn its answers into orders, so a
guard can't be skipped by a code path that forgets to call it.

Guards, cheapest first (RISK.md has the table and the reasoning behind each
number):

    KillSwitch        one switch: no new exposure, optionally flatten. Tripped
                      by the session hard stop, a reject storm, a flaky
                      connection, a stale feed, or by hand (a KILL file).
    SessionStop       session P&L: soft stop → reduce-only, hard stop → kill
                      switch + flatten.
    PositionBook      per-position stop: loss past a share of entry notional
                      → flatten that symbol and keep out of it for a while.
    notional_room     gross and net notional caps, checked before the order.
    RejectMonitor     too many rejects for the orders sent → cool off.
    ConnectionMonitor reconnect storms and a silent feed.
    ResumeGate        after a halt lifts: no new exposure until the book has
                      settled and is two-sided again.
    VenueGuard        the exchange seat's policy: per-team rate limits with a
                      penalty box, a fat-finger collar, and a reopening collar
                      after halts.

All times are exchange ticks (team.common.TickClock), so the guards behave the
same live, in the simulator and in tests on a frozen clock.
"""

from __future__ import annotations

import logging
import os
import pathlib
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

KILL_DIR = pathlib.Path(os.environ.get(
    "STT_KILL_DIR", pathlib.Path(__file__).resolve().parent.parent / "Phase2" / "state"))
KILL_FILE_POLL_TICKS = 1.0          # stat() the kill files at most this often

# Rejects that are the normal cost of quoting, not a sign that we are broken:
# a cancel racing a fill, and self-trade prevention against our own seat.
BENIGN_REJECTS = frozenset({"ORDER_NOT_FOUND", "STP_CANCEL"})


# ── Limits ──────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class RiskLimits:
    """The numbers. One instance per role; RISK.md argues each one."""

    gross_leverage: float           # gross notional ≤ this × equity
    net_leverage: float             # |net notional| ≤ this × equity
    position_stop_pct: float        # per-position loss, share of entry notional
    position_stop_min_usd: float    # …and at least this many dollars (noise floor)
    stop_cooldown_ticks: float      # no new exposure in a stopped symbol for this long
    soft_stop_pct: float            # session loss, share of seat capital → reduce-only
    hard_stop_pct: float            # session loss → kill switch + flatten
    reject_window_ticks: float = 30.0
    reject_min_count: int = 10      # a storm needs at least this many rejects…
    reject_max_ratio: float = 0.5   # …and at least this share of what we sent
    reject_cooldown_ticks: float = 60.0
    reconnect_window_ticks: float = 300.0
    reconnect_max: int = 3          # this many reconnects in the window → trip
    reconnect_cooldown_ticks: float = 120.0
    stale_feed_ticks: float = 15.0  # no market data this long while open → trip
    resume_settle_ticks: float = 3.0
    resume_max_spread_bps: float = 100.0


TRADER_LIMITS = RiskLimits(
    gross_leverage=1.5, net_leverage=1.0,
    position_stop_pct=0.04, position_stop_min_usd=150.0, stop_cooldown_ticks=60.0,
    soft_stop_pct=0.03, hard_stop_pct=0.05)

BROKER_LIMITS = RiskLimits(
    gross_leverage=1.5, net_leverage=0.5,
    position_stop_pct=0.02, position_stop_min_usd=150.0, stop_cooldown_ticks=60.0,
    soft_stop_pct=0.03, hard_stop_pct=0.05,
    # The broker sends ~5 messages a second, so a storm needs more rejects.
    reject_min_count=20)


# ── Kill switch ─────────────────────────────────────────────────────────
@dataclass
class _Trip:
    flatten: bool
    until: float | None             # None: until reset (by hand, or a new session)
    at: float


class KillSwitch:
    """The one switch that stops all new exposure, optionally flattening.

    Several causes can hold it at once; it is on while any of them is. A cause
    with `until` releases itself (a reject storm cools off), one without it
    stays until reset() — by a human, or by a new session for the automatic
    loss stop. A human trips it by creating a file in KILL_DIR:

        KILL              every seat on this machine
        KILL.<seat id>    one seat

    with the word "flatten" in it to also close every position. Deleting the
    file releases the manual trip. `python -m scripts.kill_switch` does both.
    """

    def __init__(self, seat: str, kill_dir: pathlib.Path | None = None) -> None:
        self.seat = seat
        self.kill_dir = kill_dir if kill_dir is not None else KILL_DIR
        self._trips: dict[str, _Trip] = {}
        self._polled_at = float("-inf")
        self.history: list[tuple[float, str, str]] = []   # (tick, "trip"/"release", reason)

    def trip(self, reason: str, now: float, *, flatten: bool = False,
             until: float | None = None) -> None:
        """Turn the switch on for `reason` (re-tripping only extends it)."""
        old = self._trips.get(reason)
        if old is not None:
            old.flatten = old.flatten or flatten
            old.until = None if old.until is None or until is None else max(old.until, until)
            return
        self._trips[reason] = _Trip(flatten, until, now)
        self.history.append((now, "trip", reason))
        logger.error("KILL SWITCH ON [%s] seat=%s flatten=%s %s", reason, self.seat, flatten,
                     f"until tick {until:.0f}" if until is not None else "until reset")

    def reset(self, reason: str | None = None, now: float = 0.0) -> None:
        """Release one cause, or every cause."""
        for r in [reason] if reason else list(self._trips):
            if self._trips.pop(r, None) is not None:
                self.history.append((now, "release", r))
                logger.warning("KILL SWITCH released [%s] seat=%s", r, self.seat)

    def active(self, now: float) -> bool:
        """On while any cause is; causes past their `until` are released here."""
        for r, t in list(self._trips.items()):
            if t.until is not None and now >= t.until:
                self.reset(r, now)
        return bool(self._trips)

    def flatten(self, now: float) -> bool:
        return self.active(now) and any(t.flatten for t in self._trips.values())

    def reasons(self) -> list[str]:
        return sorted(self._trips)

    def poll_file(self, now: float) -> None:
        """Pick up (or drop) a manual trip from the kill files."""
        if now - self._polled_at < KILL_FILE_POLL_TICKS:
            return
        self._polled_at = now
        found = None
        for path in (self.kill_dir / "KILL", self.kill_dir / f"KILL.{self.seat}"):
            try:
                found = path.read_text().strip().lower()
                break
            except FileNotFoundError:
                continue
            except OSError:
                found = ""                          # unreadable but present: still a kill
                break
        if found is not None:
            self.trip("manual", now, flatten="flatten" in found)
        elif "manual" in self._trips:
            self.reset("manual", now)


# ── Session stop ────────────────────────────────────────────────────────
class SessionStop:
    """Loss since the session started, against the seat's capital."""

    def __init__(self, limits: RiskLimits, capital: float) -> None:
        self.limits = limits
        self.capital = capital
        self.baseline: float | None = None
        self.soft = False
        self.hard = False
        self.pnl = 0.0

    def reset(self) -> None:
        self.baseline, self.soft, self.hard, self.pnl = None, False, False, 0.0

    def update(self, equity: float) -> tuple[bool, bool]:
        """(soft stop newly hit, hard stop newly hit). Both latch until reset()."""
        if self.baseline is None:
            self.baseline = equity
            logger.info("Session baseline equity %.2f (soft stop at %.2f, hard at %.2f)",
                        equity, equity - self.limits.soft_stop_pct * self.capital,
                        equity - self.limits.hard_stop_pct * self.capital)
        self.pnl = equity - self.baseline
        new_soft = not self.soft and self.pnl <= -self.limits.soft_stop_pct * self.capital
        new_hard = not self.hard and self.pnl <= -self.limits.hard_stop_pct * self.capital
        self.soft = self.soft or new_soft or new_hard
        self.hard = self.hard or new_hard
        return new_soft, new_hard


# ── Per-position stop ───────────────────────────────────────────────────
@dataclass
class _Entry:
    qty: int
    avg: float


class PositionBook:
    """Average entry price per symbol, from our own fills."""

    def __init__(self, limits: RiskLimits) -> None:
        self.limits = limits
        self._e: dict[str, _Entry] = {}
        self.cooldown: dict[str, float] = {}       # symbol -> no new exposure until

    def clear(self) -> None:
        self._e.clear()
        self.cooldown.clear()

    def entry(self, sym: str) -> float | None:
        e = self._e.get(sym)
        return e.avg if e and e.qty else None

    def on_fill(self, sym: str, side: str, qty: int, price: float) -> None:
        d = qty if side == "buy" else -qty
        e = self._e.get(sym)
        if e is None or e.qty == 0:
            self._e[sym] = _Entry(d, price)
            return
        new = e.qty + d
        if (e.qty > 0) == (d > 0):                  # adding: weighted average
            e.avg = (e.avg * abs(e.qty) + price * abs(d)) / abs(new)
        elif new and (new > 0) != (e.qty > 0):      # flipped through zero: new entry
            e.avg = price
        e.qty = new

    def sync(self, positions: Mapping[str, int], marks: Mapping[str, float]) -> None:
        """Agree with the venue's positions. A position we never saw filled (a
        restart, a reconnect, an IPO allocation) is entered at today's mark."""
        for sym in [s for s in self._e if not positions.get(s)]:
            del self._e[sym]
        for sym, q in positions.items():
            if not q:
                continue
            e = self._e.get(sym)
            if e is not None and e.qty == q:
                continue
            if e is not None and e.qty and (e.qty > 0) == (q > 0) and abs(q) < abs(e.qty):
                e.qty = q                           # a reduction we didn't hear about
            elif marks.get(sym):
                self._e[sym] = _Entry(q, float(marks[sym]))

    def breaches(self, positions: Mapping[str, int], marks: Mapping[str, float],
                 skip: Iterable[str] = ()) -> list[tuple[str, float]]:
        """(symbol, loss in $) for every position past its stop."""
        out = []
        skip = set(skip)
        for sym, q in positions.items():
            avg, mark = self.entry(sym), marks.get(sym)
            if not q or not avg or not mark or sym in skip:
                continue
            loss = -(mark - avg) * q
            limit = max(self.limits.position_stop_pct * abs(q) * avg,
                        self.limits.position_stop_min_usd)
            if loss >= limit:
                out.append((sym, loss))
        return out

    def cooling(self, sym: str, now: float) -> bool:
        return now < self.cooldown.get(sym, float("-inf"))


# ── Notional caps ───────────────────────────────────────────────────────
def notional_room(side: str, price: float, sym: str, positions: Mapping[str, int],
                  marks: Mapping[str, float], equity: float, limits: RiskLimits) -> int:
    """Most shares of `side` in `sym` that keep gross and net notional inside
    the caps. The part of an order that reduces the position is never limited
    here (closing risk must always be possible)."""
    pos = positions.get(sym, 0)
    reducing = max(0, -pos) if side == "buy" else max(0, pos)
    px = marks.get(sym) or price
    if px <= 0 or equity <= 0:
        return reducing
    gross = sum(abs(q) * (marks.get(s) or (price if s == sym else 0.0))
                for s, q in positions.items())
    net = sum(q * (marks.get(s) or (price if s == sym else 0.0)) for s, q in positions.items())
    # Past the reducing part, every share adds px to gross and ±px to net.
    after_reduce_gross = gross - reducing * px
    after_reduce_net = net + (reducing * px if side == "buy" else -reducing * px)
    gross_room = (limits.gross_leverage * equity - after_reduce_gross) / px
    signed = after_reduce_net if side == "buy" else -after_reduce_net
    net_room = (limits.net_leverage * equity - signed) / px
    return reducing + max(0, int(min(gross_room, net_room)))


# ── Rejects and the connection ──────────────────────────────────────────
class RejectMonitor:
    """Rejects against orders sent, over a rolling window of ticks."""

    def __init__(self, limits: RiskLimits) -> None:
        self.limits = limits
        self._sent: deque[float] = deque()
        self._rejects: deque[float] = deque()

    def _trim(self, now: float) -> None:
        w = self.limits.reject_window_ticks
        for q in (self._sent, self._rejects):
            while q and now - q[0] > w:
                q.popleft()

    def on_send(self, now: float) -> None:
        self._sent.append(now)

    def on_reject(self, code: str | None, now: float) -> bool:
        """Record a reject; True when it makes the window a storm."""
        if code in BENIGN_REJECTS:
            return False
        self._rejects.append(now)
        self._trim(now)
        n = len(self._rejects)
        return (n >= self.limits.reject_min_count
                and n >= self.limits.reject_max_ratio * max(1, len(self._sent)))

    def counts(self, now: float) -> tuple[int, int]:
        self._trim(now)
        return len(self._sent), len(self._rejects)


class ConnectionMonitor:
    """Reconnect storms, and a feed that has gone quiet while we are open."""

    def __init__(self, limits: RiskLimits) -> None:
        self.limits = limits
        self._connects: deque[float] = deque()
        self.last_data: float | None = None

    def on_connect(self, now: float) -> bool:
        """Record a (re)connect; True when there have been too many lately."""
        self._connects.append(now)
        while self._connects and now - self._connects[0] > self.limits.reconnect_window_ticks:
            self._connects.popleft()
        self.last_data = now                       # a fresh connection is fresh data
        return len(self._connects) - 1 >= self.limits.reconnect_max   # the first isn't a RE-connect

    def on_data(self, now: float) -> None:
        self.last_data = now

    def stale(self, now: float) -> bool:
        return self.last_data is not None and now - self.last_data > self.limits.stale_feed_ticks


# ── Re-entry after a halt ───────────────────────────────────────────────
class ResumeGate:
    """After TRADING_RESUMED, no new exposure in that symbol until the book has
    had time to refill and shows a two-sided market of sane width."""

    def __init__(self, limits: RiskLimits) -> None:
        self.limits = limits
        self._resumed: dict[str, float] = {}

    def clear(self) -> None:
        self._resumed.clear()

    def on_resume(self, sym: str, now: float) -> None:
        self._resumed[sym] = now
        logger.info("RESUME %s: no new exposure for %.0f ticks, then only into a two-sided book",
                    sym, self.limits.resume_settle_ticks)

    def blocks(self, sym: str, now: float, bid: float | None, ask: float | None,
               book_known: bool = True) -> str | None:
        """Why new exposure in `sym` must wait, or None once it may go. A caller
        that doesn't see the touch (book_known=False) gets the time rule only."""
        t = self._resumed.get(sym)
        if t is None:
            return None
        if now - t < self.limits.resume_settle_ticks:
            return f"resumed {now - t:.1f} ticks ago"
        if not book_known:
            del self._resumed[sym]
            return None
        if not bid or not ask or ask <= bid:
            return "book not two-sided since the resume"
        spread_bps = (ask - bid) / ((ask + bid) / 2) * 1e4
        if spread_bps > self.limits.resume_max_spread_bps:
            return f"spread {spread_bps:.0f} bps since the resume"
        del self._resumed[sym]                     # settled: the gate is done with it
        logger.info("RESUME %s: book settled (%.0f bps) — re-entering", sym, spread_bps)
        return None


# ── The facade the seats use ────────────────────────────────────────────
@dataclass
class Decision:
    """What update() wants done this tick."""

    flatten: list[str] = field(default_factory=list)   # close these symbols now
    kill: bool = False                                  # no new exposure at all
    flatten_all: bool = False                           # …and close everything


class RiskManager:
    """Every guard for one seat, behind two calls: update() and check()."""

    def __init__(self, seat: str, limits: RiskLimits, capital: float,
                 kill_dir: pathlib.Path | None = None) -> None:
        self.seat = seat
        self.limits = limits
        self.kill = KillSwitch(seat, kill_dir)
        self.session = SessionStop(limits, capital)
        self.book = PositionBook(limits)
        self.rejects = RejectMonitor(limits)
        self.conn = ConnectionMonitor(limits)
        self.resume = ResumeGate(limits)
        self.blocked = 0                            # orders check() shrank or dropped

    # ── Lifecycle ───────────────────────────────────────────────────────
    def new_session(self, now: float) -> None:
        """A new session: automatic stops start over; a manual kill does not."""
        self.session.reset()
        self.book.clear()
        self.resume.clear()
        for r in self.kill.reasons():
            if r != "manual":
                self.kill.reset(r, now)

    # ── Things that happened ────────────────────────────────────────────
    def on_send(self, now: float) -> None:
        self.rejects.on_send(now)

    def on_reject(self, code: str | None, now: float) -> None:
        if self.rejects.on_reject(code, now):
            sent, rej = self.rejects.counts(now)
            logger.error("REJECT STORM: %d rejects for %d orders in %.0f ticks (last %s)",
                         rej, sent, self.limits.reject_window_ticks, code)
            self.kill.trip("reject_storm", now, until=now + self.limits.reject_cooldown_ticks)

    def on_connect(self, now: float) -> None:
        if self.conn.on_connect(now):
            self.kill.trip("reconnects", now, until=now + self.limits.reconnect_cooldown_ticks)
        self.kill.reset("stale_feed", now)

    def on_data(self, now: float) -> None:
        self.conn.on_data(now)

    def on_fill(self, sym: str, side: str, qty: int, price: float) -> None:
        self.book.on_fill(sym, side, qty, price)

    def on_resume(self, sym: str, now: float) -> None:
        self.resume.on_resume(sym, now)

    # ── Once a tick ─────────────────────────────────────────────────────
    def update(self, now: float, equity: float | None, positions: Mapping[str, int],
               marks: Mapping[str, float], session_open: bool = True,
               skip_stops: Iterable[str] = ()) -> Decision:
        """Run the stops. Returns what to close and whether new orders may go."""
        self.kill.poll_file(now)
        if session_open and self.conn.stale(now):
            self.kill.trip("stale_feed", now)
        elif "stale_feed" in self.kill.reasons() and not self.conn.stale(now):
            self.kill.reset("stale_feed", now)
        d = Decision()
        unpriced = [s for s, q in positions.items() if q and not marks.get(s)]
        if equity is not None and unpriced:
            # Equity is unknowable this tick (a position with no price at all):
            # judging the session on it would read the holding as a total loss.
            logger.debug("Session stop skipped: no price for %s", unpriced)
            equity = None
        if equity is not None and session_open:
            soft, hard = self.session.update(equity)
            if soft:
                logger.error("SESSION SOFT STOP: P&L %.2f ≤ −%.0f%% of capital %.0f — reduce-only",
                             self.session.pnl, self.limits.soft_stop_pct * 100, self.session.capital)
            if hard:
                logger.error("SESSION HARD STOP: P&L %.2f ≤ −%.0f%% of capital %.0f — kill + flatten",
                             self.session.pnl, self.limits.hard_stop_pct * 100, self.session.capital)
                self.kill.trip("session_loss", now, flatten=True)
        self.book.sync(positions, marks)
        for sym, loss in self.book.breaches(positions, marks, skip_stops):
            if not self.book.cooling(sym, now):
                logger.error("POSITION STOP %s: %+d @ entry %.2f, mark %.2f, loss $%.2f — "
                             "flattening, no new exposure for %.0f ticks", sym, positions[sym],
                             self.book.entry(sym) or 0.0, marks[sym], loss,
                             self.limits.stop_cooldown_ticks)
            self.book.cooldown[sym] = now + self.limits.stop_cooldown_ticks
            d.flatten.append(sym)
        d.kill = self.kill.active(now)
        d.flatten_all = self.kill.flatten(now)
        return d

    # ── Before every order ──────────────────────────────────────────────
    def reduce_only(self, sym: str, now: float) -> str | None:
        """Why only position-reducing orders may go in `sym`, or None."""
        if self.kill.active(now):
            return "kill switch: " + ",".join(self.kill.reasons())
        if self.session.soft:
            return f"session soft stop (P&L {self.session.pnl:+.0f})"
        if self.book.cooling(sym, now):
            return "position stop cool-down"
        return None

    def check(self, sym: str, side: str, qty: int, price: float, now: float,
              positions: Mapping[str, int], marks: Mapping[str, float], equity: float | None,
              bid: float | None = None, ask: float | None = None,
              closing: bool = False, book_known: bool = True) -> tuple[int, str | None]:
        """(shares allowed, why it was cut or None). Closing orders (flatten,
        stop exits) pass every growth limit but are still blocked by a plain
        kill switch without the flatten flag: that one means *stop sending*."""
        pos = positions.get(sym, 0)
        reducing = max(0, -pos) if side == "buy" else max(0, pos)
        if self.kill.active(now) and not (closing and self.kill.flatten(now)):
            return self._cut(0, "kill switch: " + ",".join(self.kill.reasons()))
        if closing:                                 # never more than the position
            allowed = min(qty, reducing)
            return (allowed, None) if allowed == qty else self._cut(allowed, "closing past flat")
        why = self.reduce_only(sym, now) or self.resume.blocks(sym, now, bid, ask, book_known)
        if why is not None:
            allowed = min(qty, reducing)
            return (allowed, None) if allowed == qty else self._cut(allowed, why)
        if equity is None:
            return qty, None
        room = notional_room(side, price, sym, positions, marks, equity, self.limits)
        if room < qty:
            return self._cut(max(0, room), f"notional cap (room {room})")
        return qty, None

    def _cut(self, allowed: int, why: str) -> tuple[int, str]:
        self.blocked += 1
        return allowed, why


# ── The exchange seat ───────────────────────────────────────────────────
@dataclass(frozen=True)
class VenueLimits:
    """Venue policy numbers (RISK.md, exchange section)."""

    rate_per_tick: float = 30.0         # sustained orders per team per tick…
    rate_burst: float = 60.0            # …with this much burst
    strikes_to_box: int = 3             # rate rejects within strike_window…
    strike_window_ticks: float = 10.0
    penalty_ticks: float = 5.0          # …put the team in the box this long
    max_order_notional: float = 250_000.0
    collar_pct: float = 0.10            # limit price this far from the last trade: fat finger
    reopen_ticks: float = 10.0          # after a halt lifts…
    reopen_collar_pct: float = 0.03     # …a tighter collar…
    reopen_market_orders: bool = False  # …and no market orders


class VenueGuard:
    """accept_order() logic for our venue: stop abusive flow, never a normal
    trader. Every limit sits well above what our own and the house bots send
    in a normal session (RISK.md), so only runaway loops and fat fingers hit it.

    Exempt: house bots (they make the market) — the caller passes `exempt`."""

    def __init__(self, limits: VenueLimits | None = None,
                 clock: Callable[[], float] | None = None) -> None:
        import time as _time
        self.limits = limits or VenueLimits()
        self._now = clock or _time.monotonic
        self._bucket: dict[str, tuple[float, float]] = {}   # team -> (tokens, at)
        self._strikes: dict[str, deque[float]] = {}
        self._boxed: dict[str, float] = {}
        self._last: dict[str, float] = {}                  # symbol -> last trade price
        self._halted: set[str] = set()
        self._market_halted = False
        self._reopened: dict[str, float] = {}              # symbol -> reopen time
        self._market_reopened: float | None = None
        self.rejected: dict[str, int] = {}                 # reason -> count

    # ── Market state ────────────────────────────────────────────────────
    def on_trade(self, symbol: str, price: float) -> None:
        if price > 0:
            self._last[symbol] = price

    def on_event(self, event: str, data: Mapping) -> None:
        sym = data.get("symbol")
        if event == "TRADING_HALT" and sym:
            self._halted.add(sym)
        elif event == "TRADING_RESUMED" and sym:
            self._halted.discard(sym)
            self._reopened[sym] = self._now()
        elif event == "MARKET_HALT":
            self._market_halted = True
        elif event == "MARKET_RESUMED":
            self._market_halted = False
            self._market_reopened = self._now()

    def _reopening(self, sym: str, now: float) -> bool:
        w = self.limits.reopen_ticks
        t = self._reopened.get(sym)
        m = self._market_reopened
        return (t is not None and now - t < w) or (m is not None and now - m < w)

    # ── The check ───────────────────────────────────────────────────────
    def accept(self, team: str, symbol: str, side: str, order_type: str,
               price: float | None, quantity: int, exempt: bool = False) -> tuple[bool, str]:
        now = self._now()
        if exempt:
            return True, ""
        lim = self.limits
        until = self._boxed.get(team)
        if until is not None and now < until:
            return self._no("penalty", f"{team} is in the penalty box for "
                            f"{until - now:.0f}s more (order rate)")
        if not self._take(team, now):
            strikes = self._strikes.setdefault(team, deque())
            strikes.append(now)
            while strikes and now - strikes[0] > lim.strike_window_ticks:
                strikes.popleft()
            if len(strikes) >= lim.strikes_to_box:
                self._boxed[team] = now + lim.penalty_ticks
                strikes.clear()
                logger.warning("VENUE: %s boxed for %.0fs — repeated rate breaches",
                               team, lim.penalty_ticks)
            return self._no("rate", f"over {lim.rate_per_tick:.0f} orders/s on this venue")
        if self._market_halted or symbol in self._halted:
            return self._no("halted", f"{symbol} is halted — orders resume when it reopens")
        ref = self._last.get(symbol)
        reopening = self._reopening(symbol, now)
        if order_type == "market" and reopening and not lim.reopen_market_orders:
            return self._no("reopen_market",
                            f"{symbol} is reopening after a halt: limit orders only")
        if price and ref:
            if price * quantity > lim.max_order_notional:
                return self._no("notional", f"order notional ${price * quantity:,.0f} over "
                                f"${lim.max_order_notional:,.0f}")
            collar = lim.reopen_collar_pct if reopening else lim.collar_pct
            through = (price / ref - 1) if side == "buy" else (1 - price / ref)
            if through > collar:
                return self._no("collar", f"{symbol} {side} @ {price:.2f} is {through:.1%} "
                                f"through the last trade {ref:.2f} (max {collar:.0%}"
                                f"{' while reopening' if reopening else ''})")
        return True, ""

    def _take(self, team: str, now: float) -> bool:
        lim = self.limits
        tokens, at = self._bucket.get(team, (lim.rate_burst, now))
        tokens = min(lim.rate_burst, tokens + (now - at) * lim.rate_per_tick)
        if tokens < 1:
            self._bucket[team] = (tokens, now)
            return False
        self._bucket[team] = (tokens - 1, now)
        return True

    def _no(self, kind: str, why: str) -> tuple[bool, str]:
        self.rejected[kind] = self.rejected.get(kind, 0) + 1
        return False, why
