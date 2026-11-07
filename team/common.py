"""
STT_11 — what the trader and broker seats share.

TickClock     the exchange's logical clock; every timer in the bots runs on it
HaltTracker   which symbols are halted, from events, rejects and timeouts
leaderboard   short interest and team positions across the seats it lists
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable, Iterable

logger = logging.getLogger(__name__)

OWN_TEAM_PREFIX = "stt_11_"
EXCHANGE_TICK_SEC = 1.0         # the exchange advances one tick per second
HALT_FALLBACK_TICKS = 300       # halt length to assume if the event doesn't say
HALT_SLACK_TICKS = 5            # past that, assume we missed the resume event
_HALT_REJECT = re.compile(r"(\S+) trading is halted\. Resume in (\d+)s")


class TickClock:
    """Exchange ticks, interpolated between the ticks the venue reports.

    Timing a strategy with time.time() gives different results at different
    replay speeds, so bots read this instead. The simulator passes a frozen
    `now` and calls note() once per tick, which makes every timer exact.
    """

    def __init__(self, now: Callable[[], float] = time.monotonic,
                 sec_per_tick: float = EXCHANGE_TICK_SEC) -> None:
        self._now = now
        self._sec_per_tick = sec_per_tick
        self._tick = 0.0
        self._at = now()

    def note(self, tick: object) -> None:
        """Re-anchor on a tick the venue just reported. Ignores anything else."""
        if isinstance(tick, (int, float)) and not isinstance(tick, bool):
            self._tick, self._at = float(tick), self._now()

    def now(self) -> float:
        return self._tick + (self._now() - self._at) / self._sec_per_tick


class HaltTracker:
    """Single place that knows what is halted. All deadlines are in ticks.

    A symbol's deadline comes from the event's duration; a missed resume (a
    reconnect during the halt) expires it, and a SYMBOL_HALTED reject adds a
    halt the bot never heard about.
    """

    def __init__(self, clock: TickClock) -> None:
        self._clock = clock
        self._halted: dict[str, float] = {}
        self._market_until = 0.0

    def clear(self) -> None:
        self._halted.clear()
        self._market_until = 0.0

    def on_event(self, event: str, data: dict) -> list[str]:
        """Apply a session event. Returns the symbols it reopened."""
        now = self._clock.now()
        sym = data.get("symbol")
        if event == "TRADING_HALT" and sym:
            self._halted[sym] = now + float(data.get("duration") or HALT_FALLBACK_TICKS)
            logger.warning("HALT %s until tick %.0f", sym, self._halted[sym])
        elif event == "TRADING_RESUMED" and sym:
            self._halted.pop(sym, None)
            return [sym]
        elif event == "MARKET_HALT":
            self._market_until = now + float(data.get("duration") or 3 * HALT_FALLBACK_TICKS)
            logger.warning("MARKET HALT until tick %.0f", self._market_until)
        elif event == "MARKET_RESUMED":
            self._market_until = 0.0
            reopened = list(self._halted) or list(data.get("symbols") or [])
            self._halted.clear()
            return reopened
        return []

    def on_reject(self, message: str | None) -> None:
        """Learn about a halt from a SYMBOL_HALTED reject."""
        m = _HALT_REJECT.match(message or "")
        if m:
            self._halted[m.group(1)] = self._clock.now() + int(m.group(2))

    def is_halted(self, symbol: str) -> bool:
        now = self._clock.now()
        if now < self._market_until:
            return True
        until = self._halted.get(symbol)
        return until is not None and now < until + HALT_SLACK_TICKS

    def expired(self) -> list[str]:
        """Symbols whose deadline passed with no resume event; forgets them."""
        now = self._clock.now()
        gone = [s for s, until in self._halted.items() if now >= until + HALT_SLACK_TICKS]
        for sym in gone:
            del self._halted[sym]
            logger.warning("No TRADING_RESUMED for %s — assuming it reopened", sym)
        return gone


def _entries(leaderboard) -> Iterable[dict]:
    return list(leaderboard.traders or []) + list(leaderboard.brokers or [])


def short_interest(leaderboard) -> dict[str, int]:
    """Shares short per symbol across every seat — what the venue's borrow cap
    is measured against."""
    out: dict[str, int] = {}
    for entry in _entries(leaderboard):
        for sym, qty in (entry.get("positions") or {}).items():
            if qty < 0:
                out[sym] = out.get(sym, 0) - int(qty)
    return out


def team_positions(leaderboard, exclude: str) -> dict[str, int]:
    """Position per symbol of our seats other than `exclude` — the venue's
    position limit counts all of them together."""
    out: dict[str, int] = {}
    for entry in _entries(leaderboard):
        tid = entry.get("team_id") or ""
        if tid.startswith(OWN_TEAM_PREFIX) and tid != exclude:
            for sym, qty in (entry.get("positions") or {}).items():
                out[sym] = out.get(sym, 0) + int(qty)
    return out


def capital_baseline(net_worth: float, registered: float | None) -> float:
    """What the venue's maintenance check is measured against: the registered
    allocation, or the net worth seen now if lower (a local venue, or a seat
    that lost money in an earlier session). The leaderboard doesn't report it,
    and a wrong figure must never switch a healthy seat to reduce-only."""
    return net_worth if registered is None else min(net_worth, registered)
