"""
Phase4/leadlag_discovery.py — find the three hidden lead-lag links in the week-8 history.

The engine links three (leader → follower) pairs: every tick, the follower's
fair value is multiplied by (1 + beta · the leader's one-tick mark return from
`lag` ticks ago), |beta| 0.3–0.5, either sign, lag 3–8
(engine/exchange/server.py advance_tick, exchange/leadlag.py). The echo is
applied to ONE tick's fair value, so in the marks it shows up as a spike at
the lag that mostly reverts the next tick.

Why a plain lagged correlation fails here (the first version of this script):
the marks are smoothed (mark returns have ~0.7 autocorrelation) and share a
strong market factor, so every pair correlates at every lag. The method that
works is an EVENT STUDY on big idiosyncratic leader ticks:

  1. DuckDB scans only (session, symbol, tick, mark) out of the Parquet files
     and hands back sorted numpy columns; numpy reshapes them into a
     (sessions, symbols, ticks) cube. No pandas, no per-row Python.
  2. Scheduled events (earnings, the econ print: ±80 ticks), halts and the
     open are masked: a market-wide print moves every stock in the same tick.
  3. Each stock's return minus the average of the other candidates is its
     idiosyncratic return e. Take every tick where a candidate leader's e is
     beyond 4σ and average sign(e_leader) · e_follower at each offset 1..10.
     A link is a large t-statistic at one offset.
  4. Links are found one at a time: the strongest pair is recorded, then both
     stocks leave the candidate set and the market baseline (a high-volatility
     leader's echo would otherwise leak into everyone's baseline). The engine
     never chains links, so leaders and followers are disjoint.
  5. Each link is confirmed on the held-out half of the sessions (same lag,
     same sign), and the two earnings leaders are cross-checked on the
     earnings ramps, where the leader moves 30–60 bps a tick for ~15 ticks.

Lags are reported as OBSERVED in the tick feed (the follower moves `lag`
ticks after the leader's move appears in the mark). That is one more than the
engine's internal lag (the leader return is recorded after the follower is
marked), and it is the number a live bot needs.

    python Phase4/leadlag_discovery.py week8/week08_history week8/week08_history-3 ...

Writes Phase4/results/leadlag_links.json (read by team/leadlag.py) and
Phase4/results/leadlag_discovery.txt.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import time
from dataclasses import dataclass, field

import duckdb
import numpy as np

UNIVERSE = ["AAPL", "AMD", "AMZN", "GOOGL", "META", "MSFT", "NFLX", "NVDA", "TSLA"]
N_LINKS = 3
MAX_LAG = 10
SPIKE_Z = 4.0                          # a "big" idiosyncratic leader tick
EVENT_BEFORE, EVENT_AFTER = 3, 80      # ticks masked around each scheduled event
HALT_PAD = 12                          # ticks masked after a resume (the book reopens)
OPEN_TICKS = 40                        # the opening ticks (the starter skips them too)
OUT = pathlib.Path(__file__).resolve().parent / "results"


@dataclass
class Part:
    """One unzipped history part, as numpy cubes."""
    root: pathlib.Path
    sessions: np.ndarray               # session ids
    marks: np.ndarray                  # (S, n, T)
    ok: np.ndarray                     # (S, n, T) tick usable (no event, halt, open)
    earnings: list[tuple[int, int, str, float]] = field(default_factory=list)  # (s, tick, sym, pct)


def load_part(root: pathlib.Path) -> Part:
    """Read one part with DuckDB: marks for the universe and the events table."""
    con = duckdb.connect()
    syms = ", ".join(f"'{s}'" for s in UNIVERSE)
    df = con.execute(f"""
        SELECT session, tick, mark
        FROM read_parquet('{root}/books/*/*.parquet', hive_partitioning = 1)
        WHERE symbol IN ({syms})
        ORDER BY session, symbol, tick
    """).fetchnumpy()
    sessions = np.unique(df["session"])
    n_t = int(df["tick"].max()) + 1
    S, n = len(sessions), len(UNIVERSE)
    if len(df["mark"]) != S * n * n_t:
        raise ValueError(f"{root}: expected a full {S}×{n}×{n_t} grid, got {len(df['mark'])} rows")
    marks = np.asarray(df["mark"], dtype=float).reshape(S, n, n_t)

    ok = np.ones((S, n, n_t), dtype=bool)
    ok[:, :, :OPEN_TICKS] = False
    pos = {int(s): i for i, s in enumerate(sessions)}
    col = {s: i for i, s in enumerate(UNIVERSE)}
    part = Part(root, sessions, marks, ok)
    halted_at: dict[tuple[int, str], int] = {}
    for sess, tick, event, sym, pct in con.execute(f"""
            SELECT session, tick, event, symbol, pct
            FROM read_parquet('{root}/events/*.parquet') ORDER BY session, tick""").fetchall():
        i = pos.get(int(sess))
        if i is None:
            continue
        if event == "CALENDAR_EVENT":
            ok[i, :, max(0, tick - EVENT_BEFORE):tick + EVENT_AFTER] = False
            if sym in col and pct is not None:
                part.earnings.append((i, int(tick), sym, float(pct)))
        elif event == "TRADING_HALT" and sym in col:
            halted_at[(i, sym)] = tick
        elif event == "TRADING_RESUMED" and sym in col:
            t0 = halted_at.pop((i, sym), tick)
            ok[i, col[sym], t0:tick + HALT_PAD] = False
    for (i, sym), t0 in halted_at.items():
        ok[i, col[sym], t0:] = False
    return part


class SpikeStudy:
    """Σ, Σ², count of sign(e_leader(t)) · e_follower(t+k), over big leader ticks."""

    def __init__(self) -> None:
        n = len(UNIVERSE)
        self.s1 = np.zeros((n, n, MAX_LAG + 1))
        self.s2 = np.zeros((n, n, MAX_LAG + 1))
        self.cnt = np.zeros((n, n, MAX_LAG + 1))
        self.lead_abs = np.zeros(n)        # Σ |e_leader| at the spikes (for beta)
        self.lead_cnt = np.zeros(n)

    def add(self, part: Part, cands: list[int], sessions: slice) -> None:
        r = np.diff(np.log(part.marks[sessions]), axis=2)          # return INTO tick t+1
        g = part.ok[sessions][:, :, 1:] & part.ok[sessions][:, :, :-1]
        r0 = np.where(g, r, 0.0)
        tot, cnt = r0[:, cands].sum(1), g[:, cands].sum(1)
        T = r.shape[2]
        # idiosyncratic return of each candidate: minus the mean of the OTHER candidates
        e = np.full_like(r, np.nan)
        for i in cands:
            c = cnt - g[:, i]
            e[:, i] = np.where(g[:, i] & (c > 0), r0[:, i] - (tot - r0[:, i]) / np.maximum(c, 1), np.nan)
        for i in cands:
            sd = np.nanstd(e[:, i])
            ss, tt = np.where(np.abs(np.nan_to_num(e[:, i])) > SPIKE_Z * sd)
            keep = tt + MAX_LAG < T
            ss, tt = ss[keep], tt[keep]
            sg = np.sign(e[ss, i, tt])
            self.lead_abs[i] += np.abs(e[ss, i, tt]).sum()
            self.lead_cnt[i] += len(ss)
            for k in range(1, MAX_LAG + 1):
                v = sg[:, None] * e[ss, :, tt + k]                    # (spikes, n)
                m = ~np.isnan(v)
                v = np.where(m, v, 0.0)
                self.s1[i, :, k] += v.sum(0)
                self.s2[i, :, k] += (v * v).sum(0)
                self.cnt[i, :, k] += m.sum(0)

    def stats(self) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """(mean echo, echo-minus-revert contrast, its t-statistic), each (n, n, lag).

        The echo is a one-tick bump: + at the lag, − the tick after. Scoring
        the CONTRAST mean[k] − mean[k+1] picks the tick the echo lands on;
        scoring the largest single offset can pick the revert instead.
        """
        c = np.maximum(self.cnt, 1)
        mu = self.s1 / c
        se2 = np.maximum(self.s2 / c - mu ** 2, 1e-30) / c
        con = np.zeros_like(mu)
        t = np.zeros_like(mu)
        con[:, :, 1:MAX_LAG] = mu[:, :, 1:MAX_LAG] - mu[:, :, 2:MAX_LAG + 1]
        t[:, :, 1:MAX_LAG] = con[:, :, 1:MAX_LAG] / np.sqrt(se2[:, :, 1:MAX_LAG] + se2[:, :, 2:MAX_LAG + 1])
        return mu, con, t


def find_links(parts: list[Part], half: str) -> list[dict]:
    """Greedy: strongest (leader, follower, lag), remove both, repeat."""
    cands = list(range(len(UNIVERSE)))
    links = []
    for _ in range(N_LINKS):
        study = SpikeStudy()
        for p in parts:
            S = len(p.sessions)
            sl = {"train": slice(0, S // 2), "test": slice(S // 2, S), "all": slice(0, S)}[half]
            study.add(p, cands, sl)
        mu, con, t = study.stats()
        best = None
        for i in cands:
            for j in cands:
                if i == j:
                    continue
                k = int(np.argmax(np.abs(t[i, j, 1:MAX_LAG]))) + 1
                if best is None or abs(t[i, j, k]) > abs(t[best[0], best[1], best[2]]):
                    best = (i, j, k)
        i, j, k = best
        spike = study.lead_abs[i] / max(1, study.lead_cnt[i])
        links.append({"leader": UNIVERSE[i], "follower": UNIVERSE[j], "lag": k,
                      "sign": int(np.sign(con[i, j, k])),
                      "beta": round(float(con[i, j, k] / 2 / spike), 3),
                      "t_stat": round(float(t[i, j, k]), 1),
                      "spikes": int(study.lead_cnt[i]),
                      "echo_bps_by_offset": [round(float(x) * 1e4, 2) for x in mu[i, j, 1:]]})
        cands = [c for c in cands if c not in (i, j)]
    return links


def earnings_check(parts: list[Part], leader: str, follower: str) -> dict:
    """On the leader's earnings ramp: the follower's move relative to the
    unlinked stocks, signed by the print, at each offset from the print tick."""
    li, fi = UNIVERSE.index(leader), UNIVERSE.index(follower)
    rows = []
    for p in parts:
        lm = np.log(p.marks)
        for s, tick, sym, pct in p.earnings:
            if sym != leader or tick + 30 >= lm.shape[2]:
                continue
            others = [k for k in range(len(UNIVERSE)) if k not in (li, fi)]
            base = lm[s, :, tick - 1]
            rel = (lm[s, fi, tick:tick + 30] - base[fi]) - (lm[s, others, tick:tick + 30] - base[others, None]).mean(0)
            lead = lm[s, li, tick:tick + 30] - base[li]
            rows.append((np.sign(pct) * rel * 1e4, np.sign(pct) * lead * 1e4))
    if not rows:
        return {}
    rel = np.mean([r[0] for r in rows], axis=0)
    lead = np.mean([r[1] for r in rows], axis=0)
    first = next((k for k, v in enumerate(rel) if abs(v) > 5), None)
    return {"prints": len(rows), "leader_bps_by_offset": [round(float(x)) for x in lead[:26]],
            "follower_rel_bps_by_offset": [round(float(x), 1) for x in rel[:26]],
            "first_offset_moved": first}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("roots", nargs="+", type=pathlib.Path, help="week08_history folder(s)")
    args = ap.parse_args()
    t0 = time.perf_counter()
    parts, seen = [], set()
    for root in args.roots:
        p = load_part(root.expanduser())
        dup = seen & {str(s) for s in p.sessions}
        if dup:                                  # the same zip unpacked twice
            print(f"skipping {root}: {len(dup)} sessions already loaded")
            continue
        seen |= {str(s) for s in p.sessions}
        parts.append(p)
        print(f"loaded {root} ({len(p.sessions)} sessions) at {time.perf_counter() - t0:.1f} s")
    t_load = time.perf_counter() - t0
    rows = sum(p.marks.size for p in parts)

    found = {h: find_links(parts, h) for h in ("train", "test", "all")}
    key = lambda d: (d["leader"], d["follower"], d["lag"], d["sign"])
    confirmed = {key(d) for d in found["train"]} == {key(d) for d in found["test"]}
    links = found["all"]
    for d in links:
        if d["leader"] in ("GOOGL", "NFLX"):       # the two names with earnings prints
            d["earnings_check"] = earnings_check(parts, d["leader"], d["follower"])
    elapsed = time.perf_counter() - t0

    L = [(f"Week-8 lead-lag scan: {len(seen)} sessions, {rows:,} symbol-ticks "
         f"(9 stocks × 8 followers × lags 1–{MAX_LAG}); events ±({EVENT_BEFORE},{EVENT_AFTER}), "
         f"halts and the first {OPEN_TICKS} ticks masked; spikes = idiosyncratic ticks beyond {SPIKE_Z:g}σ."),
         "",
         f"{'leader':>6} → {'follower':<8} {'lag':>3} {'dir':>9} {'beta':>6} {'t':>7} {'spikes':>7}"]
    for d in links:
        L.append(f"{d['leader']:>6} → {d['follower']:<8} {d['lag']:>3} "
                 f"{'same' if d['sign'] > 0 else 'opposite':>9} {d['beta']:>+6.2f} {d['t_stat']:>7.1f} {d['spikes']:>7}")
        L.append(f"{'':>8}follower echo (bps) at offsets 1–{MAX_LAG}: {d['echo_bps_by_offset']}")
    L += ["", ("Held-out check: links found on the first half of every part "
          f"{'MATCH' if confirmed else 'DO NOT MATCH'} the links found on the second half.")]
    for h in ("train", "test"):
        L.append(f"  {h:5}: " + ", ".join(f"{d['leader']}→{d['follower']} lag {d['lag']} "
                                         f"{'+' if d['sign'] > 0 else '−'} (t={d['t_stat']})" for d in found[h]))
    for d in links:
        ec = d.get("earnings_check")
        if ec:
            L += ["", (f"Earnings cross-check, {d['leader']} prints ({ec['prints']}): the {d['follower']} "
                  f"move relative to the unlinked stocks starts at offset {ec['first_offset_moved']}"),
                  f"  {d['leader']} (bps, signed by the print): {ec['leader_bps_by_offset']}",
                  f"  {d['follower']} relative:               {ec['follower_rel_bps_by_offset']}"]
    L += ["", "The echo is a one-tick bump in the follower's fair value that mostly reverts the",
          "next tick (+ at the lag, − just after), so a trade must be on BEFORE the lag and",
          "off as the echo lands. In quiet markets the bump is a few bps (under the spread);",
          "on a leader's earnings ramp or a market-wide print it is 15–20 bps a tick for ~15 ticks.",
          "", (f"Pipeline: {elapsed:.1f} s end to end ({t_load:.1f} s DuckDB load, "
          f"{elapsed - t_load:.1f} s numpy) for {rows:,} rows.")]
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "leadlag_links.json").write_text(json.dumps(
        {"links": links, "lag_convention": "observed in the tick feed (engine lag + 1)",
         "confirmed_out_of_sample": confirmed, "sessions": len(seen), "rows": rows}, indent=2))
    (OUT / "leadlag_discovery.txt").write_text("\n".join(L) + "\n")
    print("\n".join(L))


if __name__ == "__main__":
    main()
