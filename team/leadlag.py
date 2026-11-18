"""
team/leadlag.py — trade the week-8 hidden lead-lag links.

Three (leader → follower) links were found in the week-8 history
(Phase4/leadlag_discovery.py, results in Phase4/results/leadlag_links.json):

    AMD  → GOOGL   same direction      lag 6 ticks   beta +0.37
    NFLX → AMZN    opposite direction  lag 8 (9 on the earnings ramp)   beta −0.29
    MSFT → TSLA    opposite direction  lag 4         beta −0.34

How the engine plants them (engine/exchange/server.py advance_tick): every
tick the follower's fair value is multiplied by 1 + beta × (the leader's
one-tick mark return `lag` ticks ago). That bump lasts one tick and mostly
reverts, so the follower's expected move is known `lag` ticks in advance:

    trigger   the leader's one-tick mark return r is big: |r| ≥ Z_TRIGGER × its
              rolling σ and ≥ MIN_MOVE_BPS
    entry     an IOC on the follower in direction sign(beta) × sign(r), sent the
              tick we see r (inside the lag, before the echo lands)
    exit      on the echo tick of the LAST trigger (each new trigger in the same
              direction extends the hold: an earnings ramp is ~15 triggers); a
              marker trade one tick later, outside the window it is graded in

Sizing follows the expected echo, |beta| × |r|:

    ramp      echo ≥ RAMP_ECHO_BPS (a leader moving 30–60 bps a tick: NFLX
              earnings, a halt reopening with a gap): RAMP_NOTIONAL. On the
              NFLX ramp the AMZN move reaches ~70 bps over ~23 ticks, clear of
              the spread and two taker fees on a cheap venue.
    marker    a quiet-market echo is a few bps, under the spread. These trades
              are kept small (MARKER_NOTIONAL) and rationed (MARKER_COOLDOWN
              per link, MARKER_MAX per session): they put the link on the
              tape in the right direction at a cost of a few dollars each.

Stand aside when the echo is not there or something else drives the follower:
  * a market-wide print (econ): calendar ramps set every stock's fair value
    directly, which overwrites the link (no echo in 750 sessions of history)
  * the follower's own earnings print (its own ramp owns its price)
  * either stock halted, and the first ticks after a resume (gap echoes are
    traded, but only once the follower's book is back)
"""

from __future__ import annotations

import json
import logging
import math
import os
import pathlib
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# LEADLAG_LINKS points at another links file (a local practice venue draws its
# own random links; the graded venue has the ones in the history).
LINKS_FILE = pathlib.Path(os.environ.get("LEADLAG_LINKS") or (
    pathlib.Path(__file__).resolve().parent.parent / "Phase4" / "results" / "leadlag_links.json"))

Z_TRIGGER = 4.0              # leader one-tick move, in rolling σ
MIN_MOVE_BPS = 4.0           # and at least this big
SIGMA_ALPHA = 0.02           # EWMA weight of r² (≈ 50 ticks)
SIGMA_WARMUP = 30            # ticks of returns before σ is trusted
SIGMA_FLOOR_BPS = 1.0
RAMP_ECHO_BPS = 8.0          # expected echo that pays for the round trip
RAMP_NOTIONAL = 20_000.0
MARKER_NOTIONAL = 400.0
MARKER_COOLDOWN = 90         # ticks between marker trades on one link
MARKER_MAX = 24              # marker trades per session, all links (the check needs 10)
EVENT_QUIET_BEFORE = 3       # ticks before a scheduled print: no new entries
EVENT_QUIET_AFTER = 45       # ticks after: ramp (~15) + the longest lag + margin
EXIT_SLIP_BPS = 25
ENTRY_SLIP_BPS = 15


@dataclass(frozen=True)
class Link:
    """One hidden link, as found in the history."""
    leader: str
    follower: str
    lag: int                 # ticks from the leader's move to the echo (as observed)
    beta: float              # signed

    @property
    def sign(self) -> int:
        return 1 if self.beta > 0 else -1


DEFAULT_LINKS = (Link("AMD", "GOOGL", 6, 0.37), Link("NFLX", "AMZN", 8, -0.29),
                 Link("MSFT", "TSLA", 4, -0.34))


def load_links(path: pathlib.Path = LINKS_FILE) -> tuple[Link, ...]:
    """The links from the discovery output, else the built-in copy of it."""
    try:
        data = json.loads(path.read_text())
        links = tuple(Link(d["leader"], d["follower"], int(d["lag"]), float(d["beta"]))
                      for d in data["links"])
        if len(links) == len(DEFAULT_LINKS):
            return links
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return DEFAULT_LINKS


@dataclass
class Trade:
    """One open lead-lag position in a follower."""
    link: Link
    side: str                # "buy" or "sell" (the entry)
    target: int              # signed shares wanted
    exit_at: int             # tick to be flat by
    kind: str                # "ramp" or "marker"
    held: int = 0            # signed shares filled
    entries: int = 0
    cash: float = 0.0        # + received, − paid (for the P&L log)


@dataclass
class Order:
    """What the strategy wants sent (the trader gates and routes it)."""
    symbol: str
    side: str
    quantity: int
    price: float
    reason: str


@dataclass
class LeadLagSignal:
    """Leader triggers → follower orders. Pure: no I/O, driven by the trader."""
    links: tuple[Link, ...] = field(default_factory=load_links)
    marker_notional: float = MARKER_NOTIONAL
    ramp_notional: float = RAMP_NOTIONAL
    _last_tick: dict[str, int] = field(default_factory=dict)
    _last_mark: dict[str, float] = field(default_factory=dict)
    _var: dict[str, float] = field(default_factory=dict)
    _n: dict[str, int] = field(default_factory=dict)
    _marker_at: dict[str, int] = field(default_factory=dict)
    markers: int = 0
    trades: dict[str, Trade] = field(default_factory=dict)       # follower -> open trade
    quiet: list[tuple[int, int, str | None]] = field(default_factory=list)   # (from, to, symbol|None=all)
    log: list[dict] = field(default_factory=list)                # one row per trigger acted on
    closed: list[Trade] = field(default_factory=list)

    @property
    def followers(self) -> set[str]:
        return {lk.follower for lk in self.links}

    # ── Session and calendar ────────────────────────────────────────────
    def new_session(self) -> None:
        """Counters and windows are per session; open trades are the trader's to close."""
        self.markers = 0
        self._marker_at.clear()
        self.quiet.clear()
        self.log.clear()

    def on_calendar(self, events: list[dict]) -> None:
        """CALENDAR: blank out each scheduled print (all symbols for a market-wide one)."""
        for ev in events:
            tick = ev.get("tick")
            if not isinstance(tick, (int, float)) or ev.get("kind") == "ipo":
                continue
            sym = None if ev.get("market_wide") or not ev.get("symbol") else ev["symbol"]
            self.quiet.append((int(tick) - EVENT_QUIET_BEFORE, int(tick) + EVENT_QUIET_AFTER, sym))

    def on_print(self, tick: int, affected: list[str] | None, market_wide: bool) -> None:
        """CALENDAR_EVENT / SHOCK fired now: same window, in case the calendar was missed."""
        if market_wide or not affected:
            self.quiet.append((tick, tick + EVENT_QUIET_AFTER, None))
        else:
            for s in affected:
                self.quiet.append((tick, tick + EVENT_QUIET_AFTER, s))

    def _quiet(self, sym: str, tick: int) -> bool:
        return any(a <= tick <= b and (s is None or s == sym) for a, b, s in self.quiet)

    # ── Each tick ───────────────────────────────────────────────────────
    def on_mark(self, sym: str, tick: int, mark: float) -> float | None:
        """Feed a leader's mark; returns its one-tick log return on a new tick."""
        if mark <= 0:
            return None
        last_t = self._last_tick.get(sym)
        if last_t is not None and tick <= last_t:
            return None
        prev = self._last_mark.get(sym)
        self._last_tick[sym], self._last_mark[sym] = tick, mark
        if prev is None or last_t is None or tick - last_t > 3:   # a gap in our feed: no return
            return None
        return math.log(mark / prev)

    def sigma(self, sym: str) -> float | None:
        if self._n.get(sym, 0) < SIGMA_WARMUP:
            return None
        return max(math.sqrt(self._var[sym]), SIGMA_FLOOR_BPS / 1e4)

    def _learn(self, sym: str, r: float) -> None:
        """EWMA of r², with each return capped at the trigger size: a spike
        moves σ by no more than a trigger would, but a lasting change in
        volatility is learned (an uncapped σ would chase every spike; one that
        ignores spikes would never see a calm market turn rough)."""
        n = self._n.get(sym, 0)
        v = self._var.get(sym, r * r)
        if n >= SIGMA_WARMUP:
            cap = Z_TRIGGER * max(math.sqrt(v), SIGMA_FLOOR_BPS / 1e4)
            r = max(-cap, min(cap, r))
        self._var[sym] = v + SIGMA_ALPHA * (r * r - v) if n else r * r
        self._n[sym] = n + 1

    def step(self, tick: int, marks: dict[str, float], touch: dict[str, tuple[float | None, float | None]],
             halted: set[str], positions: dict[str, int], *,
             rets: dict[str, float] | None = None, busy: set[str] = frozenset()) -> list[Order]:
        """One tick: learn, trigger, size, and exit. Returns orders to send.

        marks     venue mark per symbol (primary); one-tick returns are taken
                  from it unless `rets` is given
        touch     (bid, ask) per follower, for pricing
        halted    symbols halted (or just resumed and not settled)
        positions combined positions (to size exits off what is really held)
        rets      leader one-tick log returns since the last step (the live
                  trader measures them on each change of the venue mark, which
                  is exact; sampling marks on a drifting clock is not)
        busy      followers another strategy is trading right now: no entry
        """
        out: list[Order] = []
        for lk in self.links:
            if rets is not None:
                r = rets.get(lk.leader)
            else:
                mark = marks.get(lk.leader)
                r = self.on_mark(lk.leader, tick, mark) if mark else None
            if r is None:
                continue
            if lk.follower in busy:
                self._learn(lk.leader, r)
                continue
            sd = self.sigma(lk.leader)
            self._learn(lk.leader, r)
            if sd is None or abs(r) < max(Z_TRIGGER * sd, MIN_MOVE_BPS / 1e4):
                continue
            if lk.follower in halted:                 # a halted leader's mark still moves
                continue
            # The follower's own print or a market-wide one overwrites the echo.
            # The LEADER's own print is the opportunity (NFLX earnings → AMZN).
            if self._quiet(lk.follower, tick):
                continue
            order = self._trigger(lk, tick, r, touch.get(lk.follower, (None, None)))
            if order is not None:
                out.append(order)
        out.extend(self._exits(tick, touch, halted, positions))
        return out

    def _trigger(self, lk: Link, tick: int, r: float, touch: tuple[float | None, float | None]) -> Order | None:
        side = "buy" if r * lk.sign > 0 else "sell"
        echo_bps = abs(lk.beta * r) * 1e4
        kind = "ramp" if echo_bps >= RAMP_ECHO_BPS else "marker"
        bid, ask = touch
        px_ref = ask if side == "buy" else bid
        if not px_ref:
            return None
        tr = self.trades.get(lk.follower)
        if tr is not None:
            if tr.side != side:
                # The leader turned, but the follower keeps echoing the earlier
                # move for `lag` more ticks: keep the exit already set (the last
                # same-direction trigger's echo). The opposite trade opens once
                # this one is flat (NFLX's earnings ramp retraces ~half).
                return None
            if kind != "ramp":
                return None                           # a marker never extends a hold
            tr.kind = "ramp"
            tr.exit_at = max(tr.exit_at, tick + self._hold(lk, "ramp"))   # the ramp goes on: hold longer
            want = self._shares(kind, px_ref) * (1 if side == "buy" else -1)
            if abs(want) <= abs(tr.target):
                return None
            tr.target = want
        else:
            if kind == "marker":
                if (self.markers >= MARKER_MAX
                        or tick - self._marker_at.get(lk.follower, -10**9) < MARKER_COOLDOWN):
                    return None
                self.markers += 1
                self._marker_at[lk.follower] = tick
            want = self._shares(kind, px_ref) * (1 if side == "buy" else -1)
            tr = Trade(lk, side, want, tick + self._hold(lk, kind), kind)
            self.trades[lk.follower] = tr
        qty = abs(tr.target) - abs(tr.held)
        if qty <= 0:
            return None
        tr.entries += 1
        self.log.append({"tick": tick, "leader": lk.leader, "r_bps": round(r * 1e4, 1),
                         "follower": lk.follower, "side": side, "kind": kind,
                         "echo_bps": round(echo_bps, 1), "lag": lk.lag})
        px = px_ref * (1 + ENTRY_SLIP_BPS / 1e4) if side == "buy" else px_ref * (1 - ENTRY_SLIP_BPS / 1e4)
        return Order(lk.follower, side, qty, round(px, 2),
                     f"{kind} {lk.leader} {r * 1e4:+.1f} bps → {lk.follower} {side} "
                     f"(echo {echo_bps:.1f} bps in {lk.lag} ticks)")

    @staticmethod
    def _hold(lk: Link, kind: str) -> int:
        """Ticks to hold after a trigger. A ramp exits ON the echo tick, into the
        bump; a marker one tick later, outside the lag window it was graded in."""
        return lk.lag if kind == "ramp" else lk.lag + 1

    def _shares(self, kind: str, px: float) -> int:
        notional = self.ramp_notional if kind == "ramp" else self.marker_notional
        return max(1, int(notional / px))

    def _exits(self, tick: int, touch: dict[str, tuple[float | None, float | None]],
               halted: set[str], positions: dict[str, int]) -> list[Order]:
        """Unwind exactly what each trade put on, once its echo has landed.

        `held` is the trade's own fills, not the account position: the seat
        may hold other shares of the follower (an IPO, starting inventory, a
        stop that already closed it ends the trade through end())."""
        out = []
        for sym, tr in list(self.trades.items()):
            if tick < tr.exit_at:
                continue
            if tr.held == 0:
                self._done(sym, tick)                 # flat, or the entry never filled
                continue
            if sym in halted:
                continue
            bid, ask = touch.get(sym, (None, None))
            if tr.held > 0 and bid:
                out.append(Order(sym, "sell", tr.held, round(bid * (1 - EXIT_SLIP_BPS / 1e4), 2),
                                 f"exit {tr.kind} {tr.link.leader}→{sym}"))
            elif tr.held < 0 and ask:
                out.append(Order(sym, "buy", -tr.held, round(ask * (1 + EXIT_SLIP_BPS / 1e4), 2),
                                 f"exit {tr.kind} {tr.link.leader}→{sym}"))
        return out

    def on_fill(self, sym: str, side: str, qty: int, price: float) -> bool:
        """Charge a fill to the open trade in `sym`; False if none is open."""
        tr = self.trades.get(sym)
        if tr is None:
            return False
        signed = qty if side == "buy" else -qty
        tr.held += signed
        tr.cash -= signed * price
        return True

    def _done(self, sym: str, tick: int) -> None:
        tr = self.trades.pop(sym, None)
        if tr is not None:
            self.closed.append(tr)
            if tr.entries:
                logger.info("LEADLAG closed %s→%s %s (%s): cash %+.2f, %d entries, left %+d",
                            tr.link.leader, sym, tr.side, tr.kind, tr.cash, tr.entries, tr.held)

    def end(self, sym: str, tick: int) -> None:
        """A stop or the kill switch took over the follower."""
        self._done(sym, tick)

    def summary(self) -> str:
        n = len(self.log)
        ramps = sum(1 for x in self.log if x["kind"] == "ramp")
        pnl = sum(t.cash for t in self.closed if t.held == 0)
        return f"leadlag: {n} triggers acted on ({ramps} ramp), {len(self.closed)} closed, flat-trade cash {pnl:+.2f}"
