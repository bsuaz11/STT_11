#!/usr/bin/env python
"""
Phase2/backtest.py — every offline number in BACKTEST.md comes from this script.

    PYTHONPATH=engine:. python Phase2/backtest.py            # about 15 minutes
    PYTHONPATH=engine:. python Phase2/backtest.py --quick    # a smoke run
    PYTHONPATH=engine:. python Phase2/backtest.py --verify   # check stored numbers

Method
------
1. Select parameters on TRAIN_SEEDS only, by mean excess P&L.
2. Freeze them. Evaluate once on TEST_SEEDS, which played no part in step 1.
3. On the same test seeds, change one condition at a time:
     reaction delay   the look-ahead check: later entry must earn less
     leaked print     positive control: a real look-ahead must earn more
     flipped sign     placebo: the wrong direction must not make money
     pulled quotes    market makers vanish after a print, as on the real tapes
     fee schedule     sensitivity to costs
     two seats        what the second trader seat adds
4. Run the naive baselines through SimSession at the live fee.

P&L is excess over a do-nothing control holding the same granted shares in the
same session, so market drift on the grants is not counted as skill. It is net
of taker fees and carry, because it is the venue's own net worth.
"""

from __future__ import annotations

import argparse
import json
import logging
import pathlib
import random
import statistics as st
import sys
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict
from itertools import product

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT), str(ROOT / "engine")]

from Phase2.event_gap import GapParams  # noqa: E402
from Phase2.sim_harness import SessionStats, run_session  # noqa: E402

RESULTS = pathlib.Path(__file__).resolve().parent / "results"
TRAIN_SEEDS = list(range(1000, 1012))
TEST_SEEDS = list(range(2000, 2040))
GRID = {"buffer_bps": [0.0, 20.0, 50.0], "target_mult": [0.8, 1.0, 1.3],
        "max_entry_ticks": [5, 15]}


def _one(job: tuple[int, GapParams, dict]) -> SessionStats:
    logging.disable(logging.CRITICAL)
    seed, params, kwargs = job
    return run_session(seed, params, **kwargs)


def run_many(pool: ProcessPoolExecutor, seeds: list[int], params: GapParams,
             **kwargs) -> list[SessionStats]:
    return list(pool.map(_one, [(s, params, kwargs) for s in seeds]))


def summarize(rows: list[SessionStats]) -> dict:
    """The numbers the assignment asks for, across seeds."""
    excess = [r.excess for r in rows]
    plans = [p for r in rows for p in r.plans]
    wins = sum(p.pnl > 0 for p in plans)
    sd = st.stdev(excess) if len(excess) > 1 else 0.0
    mean = st.fmean(excess)
    return {
        "sessions": len(rows),
        "mean_excess": mean,
        "sd_excess": sd,
        "t_stat": mean / (sd / len(excess) ** 0.5) if sd else 0.0,
        "median_excess": st.median(excess),
        "worst_session": min(excess),
        "best_session": max(excess),
        "sessions_positive": sum(e > 0 for e in excess) / len(excess),
        "trades": len(plans),
        "hit_rate": wins / len(plans) if plans else 0.0,
        "mean_trade_pnl": st.fmean(p.pnl for p in plans) if plans else 0.0,
        "worst_drawdown": max(r.max_drawdown for r in rows),
        "mean_drawdown": st.fmean(r.max_drawdown for r in rows),
        "mean_fees": st.fmean(r.fees for r in rows),
        "mean_orders": st.fmean(r.orders for r in rows),
        "max_position": max(r.team_max_position for r in rows),
        "liquidations": sum(r.liquidated for r in rows),
        "halts_per_session": st.fmean(r.halts for r in rows),
        "rejects": _merge(r.rejects for r in rows),
    }


def _merge(dicts) -> dict[str, int]:
    out: dict[str, int] = {}
    for d in dicts:
        for k, v in d.items():
            out[k] = out.get(k, 0) + v
    return out


def select(pool: ProcessPoolExecutor, seeds: list[int], grid: dict) -> tuple[GapParams, list[dict]]:
    """Grid search on the training seeds. The test seeds never appear here."""
    table = []
    for buffer_bps, target_mult, max_entry in product(*grid.values()):
        params = GapParams(buffer_bps=buffer_bps, target_mult=target_mult,
                           max_entry_ticks=max_entry)
        table.append({"params": asdict(params), **summarize(run_many(pool, seeds, params))})
        print(f"  train buffer={buffer_bps:>4} target={target_mult} entry={max_entry:>2}: "
              f"{table[-1]['mean_excess']:+8.0f} (t={table[-1]['t_stat']:+.1f}, "
              f"trades={table[-1]['trades']})", flush=True)
    best = max(table, key=lambda r: r["mean_excess"])
    return GapParams(**best["params"]), table


def conditions(pool: ProcessPoolExecutor, seeds: list[int], params: GapParams) -> dict[str, dict]:
    """The frozen parameters under one changed condition at a time."""
    variants: dict[str, dict] = {
        "baseline (1 tick late)": {},
        "instant reaction (0 ticks)": {"delay": 0},
        "2 ticks late": {"delay": 2},
        "3 ticks late": {"delay": 3},
        "5 ticks late": {"delay": 5},
        "8 ticks late": {"delay": 8},
        "15 ticks late": {"delay": 15},
        "LEAK: told 1 tick early": {"leak": 1, "delay": 0},
        "LEAK: told 5 ticks early": {"leak": 5, "delay": 0},
        "PLACEBO: sign flipped": {"flip": True},
        "quotes pulled 8 ticks, 1 tick late": {"pull_ticks": 8},
        "quotes pulled 8 ticks, instant": {"pull_ticks": 8, "delay": 0},
        "tape fees 30/5": {"taker": 0.0030, "rebate": 0.0005},
        "engine default fees 15/10": {"taker": 0.0015, "rebate": 0.0010},
        "two seats": {"seats": 2},
    }
    out = {}
    for name, kwargs in variants.items():
        out[name] = summarize(run_many(pool, seeds, params, **kwargs))
        print(f"  {name:38s} {out[name]['mean_excess']:+8.0f} (sd {out[name]['sd_excess']:6.0f}, "
              f"t={out[name]['t_stat']:+.1f}, hit={out[name]['hit_rate']:.0%}, "
              f"trades={out[name]['trades']})", flush=True)
    return out


# ── Naive baselines, through the assignment's SimSession ─────────────────
def _momentum(symbol, prices, history, book, portfolio):
    from shared.messages import Signal
    if len(history) < 21:
        return None
    pos = portfolio["positions"].get(symbol, 0)
    change = history[-1] / history[-21] - 1
    if change > 0.003 and pos <= 0:
        return Signal(symbol=symbol, side="buy", quantity=10 - pos, price=prices[symbol])
    if change < -0.003 and pos > 0:
        return Signal(symbol=symbol, side="sell", quantity=pos, price=prices[symbol])
    return None


def _reversion(symbol, prices, history, book, portfolio):
    from shared.messages import Signal
    if len(history) < 31:
        return None
    window = history[-30:]
    mean = sum(window) / 30
    sd = st.pstdev(window)
    pos = portfolio["positions"].get(symbol, 0)
    if sd and history[-1] < mean - 2 * sd and pos <= 0:
        return Signal(symbol=symbol, side="buy", quantity=10 - pos, price=prices[symbol])
    if pos > 0 and history[-1] >= mean:
        return Signal(symbol=symbol, side="sell", quantity=pos, price=prices[symbol])
    return None


def _event_gap_no_events(symbol, prices, history, book, portfolio):
    """The signal in a market with no prints: it has nothing to act on."""
    return None


def _simsession(job: tuple[str, int, float]) -> dict:
    logging.disable(logging.CRITICAL)
    name, seed, fee = job
    from sim.session import SimSession
    import plugins.securities.defaults as securities
    securities.FUNDAMENTAL_SEED = f"phase2-{seed}"     # or every seed is the same path
    fn = {"momentum": _momentum, "reversion": _reversion, "event_gap": _event_gap_no_events}[name]
    result = SimSession().run(n_ticks=5000, strategies=[(name, fn)], symbols=["AAPL", "TSLA"],
                              fee_rate=fee, seed=seed, verbose=False)
    trader = result.traders[0]
    curve = trader.pnl_curve
    peak, dd = curve[0], 0.0
    for v in curve:
        peak = max(peak, v)
        dd = max(dd, peak - v)
    return {"pnl": curve[-1] - curve[0], "fills": len(trader.fills), "dd": dd}


def baselines(pool: ProcessPoolExecutor, seeds: list[int], fee: float) -> dict[str, dict]:
    out = {}
    for name in ("momentum", "reversion", "event_gap"):
        rows = list(pool.map(_simsession, [(name, s, fee) for s in seeds]))
        pnl = [r["pnl"] for r in rows]
        out[name] = {"mean_pnl": st.fmean(pnl), "sd_pnl": st.stdev(pnl),
                     "sessions_positive": sum(p > 0 for p in pnl) / len(pnl),
                     "mean_fills": st.fmean(r["fills"] for r in rows),
                     "worst_drawdown": max(r["dd"] for r in rows)}
        print(f"  SimSession {name:10s} {out[name]['mean_pnl']:+9.0f} (sd {out[name]['sd_pnl']:6.0f}, "
              f"fills {out[name]['mean_fills']:.0f})", flush=True)
    return out


def verify(workers: int) -> None:
    """Prove the stored baseline reproduces from code and seeds alone."""
    stored = json.loads((RESULTS / "backtest_results.json").read_text())
    params = GapParams(**stored["frozen_params"])
    expected = {s["seed"]: s["excess"] for s in stored["sessions"]}
    with ProcessPoolExecutor(workers, max_tasks_per_child=1) as pool:
        rows = run_many(pool, list(expected), params)
    wrong = [r.seed for r in rows if abs(r.excess - expected[r.seed]) > 1e-6]
    print(f"{len(rows) - len(wrong)}/{len(rows)} seeds reproduce exactly" +
          (f"; different: {wrong}" if wrong else ""))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--quick", action="store_true", help="few seeds, small grid: smoke run only")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--verify", action="store_true",
                    help="re-run the held-out seeds and compare with the stored results")
    args = ap.parse_args()
    if args.verify:
        verify(args.workers)
        return
    random.seed(0)
    train, test, grid = TRAIN_SEEDS, TEST_SEEDS, GRID
    if args.quick:
        train, test = train[:2], test[:2]
        grid = {"buffer_bps": [20.0], "target_mult": [1.0], "max_entry_ticks": [15]}
    RESULTS.mkdir(exist_ok=True)
    with ProcessPoolExecutor(args.workers, max_tasks_per_child=1) as pool:
        print(f"Selecting on {len(train)} training seeds", flush=True)
        params, table = select(pool, train, grid)
        print(f"Frozen: {params}\nEvaluating on {len(test)} held-out seeds", flush=True)
        results = conditions(pool, test, params)
        print("Baselines", flush=True)
        base = baselines(pool, test[:20], 0.004)
        sessions = run_many(pool, test, params)
    payload = {"train_seeds": train, "test_seeds": test, "frozen_params": asdict(params),
               "train_grid": table, "conditions": results, "simsession_baselines": base,
               "sessions": [{"seed": s.seed, "excess": s.excess, "pnl": s.pnl,
                             "max_drawdown": s.max_drawdown, "fees": s.fees,
                             "plans": [asdict(p) for p in s.plans]} for s in sessions],
               "curves": {str(s.seed): [e - c for e, c in zip(s.equity, s.control)]
                          for s in sessions}}
    name = "backtest_quick.json" if args.quick else "backtest_results.json"
    (RESULTS / name).write_text(json.dumps(payload))
    print(f"Wrote {RESULTS / name}")


if __name__ == "__main__":
    main()
