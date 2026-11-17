"""
Phase3/stress_session.py — the risk layer under a shock session, on the real engine.

Runs the real trader seats (team/trader.py, which includes team/risk.py)
inside the course simulator (HeadlessArena: the production ExchangeServer on
a virtual clock, with house bots) on the week-6 tournament calendar (circuit
breakers on, two earnings prints, the VLTA IPO, a dividend and a market-wide
econ print), and adds on top of it:

    unscheduled shocks   step moves large enough to fire the circuit breaker
                         (the velocity halt is 5%), mid-session, unannounced
    a manual kill        a human drops KILL.<seat> ("flatten") partway through
                         and removes it later
    --limits tight       shrinks the session stops so that they fire in this
                         session (to prove the path end to end; the real
                         thresholds are in team/risk.py)
    --limits off         every automatic guard out of reach: the same session
                         without the risk layer, to measure what it costs

and records what a grader asks: no crash, no reject storm, no runaway
position, no orders into a halt, what each guard did and when.

    PYTHONPATH=engine:. python Phase3/stress_session.py --seed 7
    PYTHONPATH=engine:. python Phase3/stress_session.py --seed 11 --limits tight
    PYTHONPATH=engine:. python Phase3/stress_session.py --seed 7 --limits off

Writes Phase3/results/stress_seed<N>[_tight|_off].{log,json}.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import logging
import os
import pathlib
import random
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
for _p in (str(ROOT), str(ROOT / "engine")):
    if _p not in sys.path:
        sys.path.insert(0, _p)
os.environ.setdefault("SEASON_DIR", str(ROOT / "season"))
os.environ.setdefault("TEAM_ID", "stt_11_trader_1")

import exchange.scenario as scenario_mod
import plugins.securities.defaults as securities
from exchange import config
from sim.arena import HeadlessArena, ScheduledShock
from sim.bots import DEFAULT_POPULATION, make_population

import arena as _arena  # noqa: F401  (puts engine/ on the path, loads .env)
from Phase2.event_gap import GapParams
from Phase2.sim_harness import (
    LIVE_HALT_TICKS,
    LIVE_TAKER,
    SeatBot,
)
from team import risk

RESULTS = pathlib.Path(__file__).resolve().parent / "results"
TICKS = 2100                    # past the L1 market halt the t1100 shock sets off (900 ticks)
# Unscheduled, unannounced step moves: each is past the 5% velocity halt.
SHOCKS = [ScheduledShock(tick=180, pct=-0.09, symbol="AMD", label="AMD −9% (unscheduled)"),
          ScheduledShock(tick=420, pct=0.07, symbol="NFLX", label="NFLX +7% (unscheduled)"),
          ScheduledShock(tick=1100, pct=-0.06, symbol=None, label="MARKET −6% (unscheduled)")]
MANUAL_KILL = (700, 760, "stt_11_trader_2")      # trip at, release at, seat


class _TickFormatter(logging.Formatter):
    def __init__(self, arena_ref: list) -> None:
        super().__init__("t%(tick)05d  %(levelname)-7s %(name)-12s %(message)s")
        self._arena = arena_ref

    def format(self, record: logging.LogRecord) -> str:
        record.tick = self._arena[0].server.tick if self._arena and self._arena[0] else 0
        return super().format(record)


def _gross_net(seat: SeatBot, arena: HeadlessArena) -> tuple[float, float, float]:
    pos = seat.portfolio.positions
    gross = sum(abs(q) * arena.ref(s) for s, q in pos.items() if q)
    net = sum(q * arena.ref(s) for s, q in pos.items() if q)
    return gross, net, arena.net_worth(seat.team_id)


def _forward_rejects(seat: SeatBot, out: list, srv) -> None:
    """Hand venue rejects to the seat, as the live socket tap does, so the
    reject-storm monitor sees them."""
    send = seat.client.send

    async def tapped(data: str) -> None:
        await send(data)
        if '"type":"error"' in data:
            msg = json.loads(data)
            out.append([srv.tick, msg.get("code"), msg.get("message")])
            seat.trader.on_reject(msg.get("code"), msg.get("message"))
    seat.client.send = tapped


LIMITS = {
    "real": risk.TRADER_LIMITS,
    "tight": dataclasses.replace(risk.TRADER_LIMITS, soft_stop_pct=0.01, hard_stop_pct=0.02),
    "off": dataclasses.replace(risk.TRADER_LIMITS, gross_leverage=1e9, net_leverage=1e9,
                               position_stop_pct=1e9, soft_stop_pct=1e9, hard_stop_pct=1e9,
                               reject_min_count=10**9, reconnect_max=10**9,
                               stale_feed_ticks=1e9, resume_settle_ticks=0.0,
                               resume_max_spread_bps=1e9),
}


async def _run(seed: int, limits: str, log: logging.Logger, arena_ref: list) -> dict:
    random.seed(seed)
    securities.FUNDAMENTAL_SEED = f"phase3-{seed}"
    config.TAKER_FEE_RATE, config.MAKER_REBATE_RATE = LIVE_TAKER, 0.0
    scen = scenario_mod.load_week(6)
    scen.apply()
    calendar = scenario_mod.scenario_shocks(scen, TICKS)

    seats = [SeatBot("stt_11_trader_1", GapParams(), {"signal", "probe", "ipo"}),
             SeatBot("stt_11_trader_2", GapParams(), {"signal", "ipo"})]
    for s in seats:
        r = s.trader.risk
        r.limits = r.session.limits = r.book.limits = r.rejects.limits = LIMITS[limits]
        r.conn.limits = r.resume.limits = LIMITS[limits]
    arena = HeadlessArena(seats + make_population(DEFAULT_POPULATION, schedule=calendar),
                          shocks=SHOCKS, seed=seed, scenario=scen, fire_shocks=True)
    arena_ref.append(arena)
    srv = arena.server
    config.HALT_DURATION_SEC = LIVE_HALT_TICKS
    srv.circuit_breaker.HALT_DURATION_SEC = LIVE_HALT_TICKS
    real_resume, resume_at = srv._resume_symbol, {}

    async def resume_by_tick(symbol: str, delay_sec: float) -> None:
        resume_at[symbol] = srv.tick + LIVE_HALT_TICKS
    srv._resume_symbol = resume_by_tick
    # Market-wide halts resume on a wall-clock sleep in the engine: by tick here.
    # (The engine spawns the resume as a background task, which the simulator
    # may only run ticks later, so time it from the halt itself.)
    real_resume_market, market_resume_at = srv._resume_market, []
    real_halt_all, halted_at = srv._halt_all_symbols, [0]

    async def halt_all(reason: str, level: int) -> None:
        halted_at[0] = srv.tick
        await real_halt_all(reason, level)

    async def resume_market_by_tick(level: int, duration: float) -> None:
        market_resume_at.append((halted_at[0] + int(duration), level))
    srv._halt_all_symbols = halt_all
    srv._resume_market = resume_market_by_tick

    # What the seat tried to do while a symbol was halted (should be nothing).
    into_halt = {s.team_id: 0 for s in seats}
    order = arena.order

    async def watched(bot, sym, side, qty, px, kind="ioc"):
        if srv.circuit_breaker.is_halted(sym):
            into_halt[bot.team_id] = into_halt.get(bot.team_id, 0) + 1
        return await order(bot, sym, side, qty, px, kind)
    arena.order = watched

    peak = {s.team_id: {"gross_x": 0.0, "net_x": 0.0, "pos": 0} for s in seats}
    halts, crashes = [], 0
    was_halted, was_market = set(), False             # to log each halt once, as it starts
    kill_at, release_at, kill_seat = MANUAL_KILL
    try:
        await arena.open_session()
        start = {s.team_id: arena.net_worth(s.team_id) for s in seats}
        reject_log: dict[str, list] = {s.team_id: [] for s in seats}
        for s in seats:
            s.listen()
            _forward_rejects(s, reject_log[s.team_id], srv)
        for _ in range(TICKS):
            t = srv.tick
            if t == kill_at:
                (risk.KILL_DIR / f"KILL.{kill_seat}").write_text("flatten")
                log.warning("HUMAN: dropped KILL.%s (flatten)", kill_seat)
            if t == release_at:
                (risk.KILL_DIR / f"KILL.{kill_seat}").unlink()
                log.warning("HUMAN: removed KILL.%s", kill_seat)
            try:
                await arena.step()
            except Exception:
                crashes += 1
                log.exception("step raised")
            for sym, at in list(resume_at.items()):
                if srv.tick >= at:
                    del resume_at[sym]
                    await real_resume(sym, 0)
            for item in list(market_resume_at):
                if srv.tick >= item[0]:
                    market_resume_at.remove(item)
                    await real_resume_market(item[1], 0)
            now_halted = {sym for sym in srv.books if sym in srv.circuit_breaker.halted}
            if srv.circuit_breaker.market_halted and not was_market:
                halts.append((srv.tick, "MARKET"))
            was_market = srv.circuit_breaker.market_halted
            halts.extend((srv.tick, sym) for sym in sorted(now_halted - was_halted))
            was_halted = now_halted
            for s in seats:
                gross, net, eq = _gross_net(s, arena)
                pk = peak[s.team_id]
                if eq > 0:
                    pk["gross_x"] = max(pk["gross_x"], gross / eq)
                    pk["net_x"] = max(pk["net_x"], abs(net) / eq)
                pk["pos"] = max([pk["pos"]] + [abs(q) for q in s.portfolio.positions.values()])
        out = {"seed": seed, "limits": limits, "ticks": TICKS, "crashes": crashes,
               "shocks": [sh.describe() for sh in SHOCKS], "halts": halts,
               "manual_kill": {"seat": kill_seat, "on": kill_at, "off": release_at}, "seats": {}}
        for s in seats:
            r = s.trader.risk
            out["seats"][s.team_id] = {
                "pnl": round(arena.net_worth(s.team_id) - start[s.team_id], 2),
                "liquidated": s.liquidated or s.portfolio.liquidated,
                "orders": s.orders, "rejects": dict(s.client.errors),
                "orders_into_halted_symbol": into_halt.get(s.team_id, 0),
                "max_abs_position": peak[s.team_id]["pos"],
                "peak_gross_x_equity": round(peak[s.team_id]["gross_x"], 3),
                "peak_net_x_equity": round(peak[s.team_id]["net_x"], 3),
                "orders_cut_by_risk": r.blocked,
                "kill_switch_history": r.kill.history,
                "session_soft_stop": r.session.soft, "session_hard_stop": r.session.hard,
                "final_positions": {k: v for k, v in s.portfolio.positions.items() if v},
                "reject_messages": reject_log[s.team_id],
            }
        return out
    finally:
        arena.close()
        scen.restore()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--limits", choices=sorted(LIMITS), default="real",
                    help="real thresholds, tight session stops, or every guard off")
    args = ap.parse_args()

    RESULTS.mkdir(parents=True, exist_ok=True)
    stem = f"stress_seed{args.seed}{'' if args.limits == 'real' else '_' + args.limits}"
    risk.KILL_DIR = pathlib.Path(tempfile.mkdtemp(prefix="stt11_kill_"))
    arena_ref: list = []
    handler = logging.FileHandler(RESULTS / f"{stem}.log", mode="w")
    handler.setFormatter(_TickFormatter(arena_ref))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.WARNING)
    for name in ("team", "Phase2"):                 # our bots at INFO, the engine quiet
        logging.getLogger(name).setLevel(logging.INFO)
    log = logging.getLogger("team.stress")
    out = asyncio.run(_run(args.seed, args.limits, log, arena_ref))
    (RESULTS / f"{stem}.json").write_text(json.dumps(out, indent=2, default=str))
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
