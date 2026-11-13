"""
Phase2/sim_harness.py — run the real trader seat against the real matching engine.

`HeadlessArena` is the course simulator: the production ExchangeServer on a
virtual clock, with a population of house bots. `SeatBot` plugs team/trader.py's
MyTrader into it unchanged, so the backtest exercises the same signal, the same
hard limits and the same order path as the live bot.

Three things are corrected to match the live venue, because the simulator's
defaults would flatter the strategy:

  fees    the engine defaults to 15/10 bps; the live venue charges 40/0
  halts   the simulator shortens halts to 0.05 s of wall time; live they last
          300 ticks, during which a ramp finishes and the position can't exit
  prints  drawn up front per seed, so every variant of a seed (delayed, leaked,
          flipped) faces the identical market events

The seat learns of a print only from the broadcast CALENDAR_EVENT, exactly as
it would over the socket. The one exception is `leak`, a deliberate look-ahead
used to prove the harness would detect one.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import random
import sys
from dataclasses import dataclass, field
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parent.parent
for _p in (str(ROOT), str(ROOT / "engine")):
    if _p not in sys.path:
        sys.path.insert(0, _p)
os.environ.setdefault("SEASON_DIR", str(ROOT / "season"))
os.environ.setdefault("TEAM_ID", "stt_11_trader_1")

import arena as _arena  # noqa: E402,F401  (puts engine/ on the path, loads .env)
import exchange.config as config  # noqa: E402
import exchange.scenario as scenario_mod  # noqa: E402
import plugins.securities.defaults as securities  # noqa: E402
from sim.arena import HeadlessArena  # noqa: E402
from sim.bots import DEFAULT_POPULATION, SimBot, make_population  # noqa: E402

from Phase2.event_gap import GapParams, PlanResult  # noqa: E402
from team.common import TickClock  # noqa: E402
from team.trader import MyTrader  # noqa: E402

LIVE_HALT_TICKS = 300
LIVE_TAKER = 0.0040
LIVE_REBATE = 0.0
DEFAULT_TICKS = 1400            # past the last print in the week 6 calendar


class _Market:
    """on_tick()'s market surface, read from the arena."""

    def __init__(self, arena: HeadlessArena) -> None:
        self._arena = arena

    def symbols(self) -> list[str]:
        return list(self._arena.symbols)

    def mid_price(self, symbol: str) -> float | None:
        return self._arena.mid(symbol) or None

    def best_bid(self, symbol: str) -> float | None:
        return self._arena.best_bid(symbol)

    def best_ask(self, symbol: str) -> float | None:
        return self._arena.best_ask(symbol)


class _Portfolio:
    """on_tick()'s portfolio surface, read from the venue's own record."""

    def __init__(self, bot: SimBot) -> None:
        self._bot = bot

    @property
    def cash(self) -> float:
        return self._bot.portfolio.cash

    @property
    def positions(self) -> dict[str, int]:
        return self._bot.portfolio.positions

    def can_buy(self, symbol: str, qty: int, price: float) -> bool:
        return self.cash >= qty * price * 1.005


class SeatBot(SimBot):
    """A team trader seat inside the simulator.

    `delay`  ticks of blindness after a print before the seat may act. 0 is an
             instant reaction, 1 (the baseline) is one tick of latency.
    `flip`   hand the seat the WRONG sign (placebo: the edge must disappear).
    `leak`   hand the seat each print this many ticks EARLY (positive control).
    """

    preset = "seat"
    role = "trader"

    def __init__(self, team_id: str, params: GapParams, roles: set[str], delay: int = 1,
                 flip: bool = False, leak: int = 0, cash: float = 100_000.0) -> None:
        super().__init__(team_id, cash)
        self.trader = MyTrader(clock=TickClock(now=lambda: 0.0), roles=roles,
                               team_id=team_id, params=params)
        self.delay, self.flip, self.leak = delay, flip, leak
        self.prints: dict[str, int] = {}        # symbol -> tick of its last print
        self.schedule: list[dict] = []          # only read when leak > 0
        self.max_position = 0
        self.orders = 0
        self.liquidated = False
        self._session_opened = False

    def listen(self) -> None:
        """Tap the seat's socket so it sees what the venue broadcasts to it."""
        send = self.client.send

        async def tapped(data: str) -> None:
            await send(data)
            if '"session_event"' in data:
                self._on_session_event(json.loads(data))
            elif '"trade_execution"' in data:
                self._on_trade(json.loads(data))
        self.client.send = tapped

    def _on_session_event(self, msg: dict) -> None:
        event, data = msg["event"], msg.get("data") or {}
        if event == "CALENDAR_EVENT":
            for sym in data.get("affected") or []:
                self.prints[sym] = self.arena.server.tick
                self.trader._pending[sym] = self.arena.server.tick + self.delay
            if self.leak:
                return                          # already delivered early
            if self.flip:
                data = dict(data, pct=-data["pct"])
        elif event == "LIQUIDATION" and data.get("team_id") == self.team_id:
            self.liquidated = True
        elif event in ("SESSION_OPEN", "SESSION_CLOSED"):
            return
        self.trader.on_event(event, msg.get("message", ""), data)

    def _on_trade(self, msg: dict) -> None:
        side = ("buy" if msg.get("buyer_id") == self.team_id else
                "sell" if msg.get("seller_id") == self.team_id else None)
        if side:
            self.trader.on_fill(side, msg["symbol"], int(msg["quantity"]), float(msg["price"]))

    def ipo_indication(self, deal: dict[str, Any], cash: float) -> tuple[int, float] | None:
        """The seat's on_ipo answer, as the simulator asks for it."""
        lo, hi = deal["offer_range"]
        self.trader._cash = cash
        wish = self.trader.on_ipo(deal["symbol"], lo, hi, deal["shares"], deal)
        return wish if isinstance(wish, tuple) else None

    async def act(self, arena: HeadlessArena) -> None:
        """One tick: feed the seat the market, then send what it decides."""
        t = self.trader
        tick = arena.server.tick
        t.clock.note(tick)
        if t.portfolio is None:
            t.portfolio = _Portfolio(self)
        if not self._session_opened:
            self._session_opened = True
            t.on_open({"scenario": arena.scenario.to_dict(), "tick": tick})
        for sym in arena.symbols:
            t.on_book(sym, arena.ref(sym))
        for ev in self.schedule:                # leak > 0 only
            if not ev["done"] and tick >= ev["tick"] - self.leak:
                ev["done"] = True
                t.on_event("CALENDAR_EVENT", "leaked", {"pct": ev["pct"], "affected": list(arena.symbols) if ev["market_wide"] else [ev["symbol"]], "tick": tick})
        market, portfolio = _Market(arena), _Portfolio(self)
        signal = t.on_tick(market, portfolio)
        if signal is not None:
            gated = t.gate(signal, market, portfolio)
            if gated is not None:
                t.note_sent(gated)
                self.orders += 1
                await arena.order(self, gated.symbol, gated.side, gated.quantity, gated.price, "ioc")
        if self.portfolio.positions:
            self.max_position = max(self.max_position,
                                    max(abs(q) for q in self.portfolio.positions.values()))


def _pull_quotes_after_prints(bot: SimBot, seat: SeatBot, ticks: int) -> None:
    """Make a market maker behave as the real tapes show: gone from a symbol
    for `ticks` after its print, then back at the new level."""
    act = bot.act
    everything = bot._symbols

    async def pulled(arena: HeadlessArena) -> None:
        gone = {s for s, t in seat.prints.items() if 0 <= arena.server.tick - t < ticks}
        for sym in gone:
            arena.cancel_all(bot, sym)
        universe = everything if everything is not None else arena.symbols
        bot._symbols = [s for s in universe if s not in gone]
        await act(arena)
    bot.act = pulled


@dataclass
class SessionStats:
    """Everything one simulated session produced."""

    seed: int
    pnl: float                  # net worth change, net of all fees and carry
    excess: float               # pnl minus the do-nothing control's pnl
    max_drawdown: float
    fees: float
    orders: int
    plans: list[PlanResult] = field(default_factory=list)
    equity: list[float] = field(default_factory=list)
    control: list[float] = field(default_factory=list)
    max_position: int = 0
    team_max_position: int = 0
    liquidated: bool = False
    rejects: dict[str, int] = field(default_factory=dict)
    halts: int = 0
    ipo_bids: int = 0


async def _run(seed: int, params: GapParams, week: int, ticks: int, delay: int, flip: bool,
               leak: int, seats: int, pull_ticks: int, taker: float, rebate: float,
               roles: set[str]) -> SessionStats:
    random.seed(seed)
    securities.FUNDAMENTAL_SEED = f"phase2-{seed}"
    saved = (config.TAKER_FEE_RATE, config.MAKER_REBATE_RATE)
    config.TAKER_FEE_RATE, config.MAKER_REBATE_RATE = taker, rebate
    scen = scenario_mod.load_week(week)
    scen.apply()
    shocks = scenario_mod.scenario_shocks(scen, ticks)

    seat_bots = [SeatBot(f"stt_11_trader_{i + 1}", params, roles, delay, flip, leak)
                 for i in range(seats)]
    field_bots = make_population(DEFAULT_POPULATION, schedule=shocks)
    for bot in seat_bots:
        bot.trader.signal.set_fee(taker)
    if pull_ticks:
        for bot in field_bots:
            if bot.role == "broker" and bot.preset != "leveraged_punter":
                _pull_quotes_after_prints(bot, seat_bots[0], pull_ticks)
    arena = HeadlessArena(seat_bots + field_bots, shocks=shocks, seed=seed, scenario=scen,
                          fire_shocks=False)
    # Live-length halts on the virtual clock: the arena resumes them by tick.
    resume_at: dict[str, int] = {}
    srv = arena.server
    real_resume = srv._resume_symbol
    config.HALT_DURATION_SEC = LIVE_HALT_TICKS
    srv.circuit_breaker.HALT_DURATION_SEC = LIVE_HALT_TICKS
    halts = [0]

    async def resume_by_tick(symbol: str, delay_sec: float) -> None:
        resume_at[symbol] = srv.tick + LIVE_HALT_TICKS
        halts[0] += 1
    srv._resume_symbol = resume_by_tick

    try:
        await arena.open_session()
        rng = random.Random(f"prints-{seed}")
        drawn = []
        for ev in srv.calendar.events:
            if ev.kind in ("earnings", "econ_print"):
                lo, hi = ev.magnitude_range
                sign = -1.0 if str(ev.forced_direction or "").lower().startswith(("d", "-", "n")) else 1.0
                pct = rng.uniform(float(lo), float(hi)) * sign
                ev.resolve = lambda pct=pct: pct
                drawn.append({"tick": srv.session_open_tick + ev.tick, "pct": pct,
                              "symbol": ev.symbol, "market_wide": ev.market_wide, "done": False})
        for bot in seat_bots:
            bot.listen()
            if leak:
                bot.schedule = [dict(d) for d in drawn]
        team_max = 0
        for _ in range(ticks):
            await arena.step()
            for sym, at in list(resume_at.items()):
                if srv.tick >= at:
                    del resume_at[sym]
                    await real_resume(sym, 0)
            if seats > 1:
                syms = {s for b in seat_bots for s in b.portfolio.positions}
                team_max = max([team_max] + [abs(sum(b.portfolio.positions.get(s, 0) for b in seat_bots))
                                             for s in syms])
        equity = [sum(vals) for vals in zip(*(arena.equity[b.team_id] for b in seat_bots), strict=False)]
        control = [c * seats for c in arena.equity["control"]]
        curve = [e - c for e, c in zip(equity, control, strict=False)]
        peak, dd = curve[0], 0.0
        for v in curve:
            peak = max(peak, v)
            dd = max(dd, peak - v)
        deal = next(iter(getattr(srv, "ipos", {}).values()), None)
        rejects: dict[str, int] = {}
        for b in seat_bots:
            for code, n in b.client.errors.items():
                rejects[code] = rejects.get(code, 0) + n
        return SessionStats(
            seed=seed, pnl=equity[-1] - equity[0],
            excess=(equity[-1] - equity[0]) - (control[-1] - control[0]),
            max_drawdown=dd, fees=sum(b.portfolio.total_fees_paid for b in seat_bots),
            orders=sum(b.orders for b in seat_bots),
            plans=[r for b in seat_bots for r in b.trader.signal.closed if r.traded],
            equity=[e - equity[0] for e in equity], control=[c - control[0] for c in control],
            max_position=max(b.max_position for b in seat_bots),
            team_max_position=team_max or max(b.max_position for b in seat_bots),
            liquidated=any(b.liquidated or b.portfolio.liquidated for b in seat_bots),
            rejects=rejects, halts=halts[0],
            ipo_bids=sum(1 for b in seat_bots if deal and b.team_id in deal.subs))
    finally:
        arena.close()
        scen.restore()
        config.TAKER_FEE_RATE, config.MAKER_REBATE_RATE = saved


def run_session(seed: int, params: GapParams | None = None, week: int = 6,
                ticks: int = DEFAULT_TICKS, delay: int = 1, flip: bool = False, leak: int = 0,
                seats: int = 1, pull_ticks: int = 0, taker: float = LIVE_TAKER,
                rebate: float = LIVE_REBATE, roles: set[str] | None = None) -> SessionStats:
    """One simulated session under one set of conditions.

    Run one per process: the engine keeps globals between sessions (the listed
    IPO, for one), so a second session in the same process is not comparable.
    backtest.py gives each session its own worker for this reason.
    """
    return asyncio.run(_run(seed, params or GapParams(), week, ticks, delay, flip, leak, seats,
                            pull_ticks, taker, rebate, roles or {"signal"}))


if __name__ == "__main__":
    logging.disable(logging.WARNING)
    s = run_session(1)
    print(f"pnl={s.pnl:+.0f} excess={s.excess:+.0f} dd={s.max_drawdown:.0f} fees={s.fees:.0f} "
          f"orders={s.orders} halts={s.halts} rejects={s.rejects} maxpos={s.max_position}")
    for p in s.plans:
        print(p)
