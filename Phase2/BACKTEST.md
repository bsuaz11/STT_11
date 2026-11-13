# BACKTEST: EventGap, a post-print fair-value signal (Trader role)

**Team STT_11:** Megan Maloney, Brett Suazo, Hannah Kong

**Reproduce:** `PYTHONPATH=engine:. python Phase2/backtest.py` (about 15 min; `--verify` re-runs all held-out seeds against the stored results). Real books: `tape_replay.py` (Weeks 2/4 tapes in `homework4/`, Weeks 5/6 in `data/`). Live: `live_compare.py`, `replay_wire_check.py`. Outputs are in `results/`. **Evidence it ran:** `results/test_output.txt` (255 tests, lint clean), `results/verify.txt`, the rehearsal logs in `results/rehearsal_logs.tgz`, and the live tapes themselves.

## 1. Hypothesis

A `CALENDAR_EVENT` publishes a signed move, so the new fair value is known at once: `pre_mark × (1 + pct)`. Resting quotes do not reprice instantly. **Claim: orders still on the wrong side of the new fair value by more than the round-trip fee are stale, and taking them is profitable net of fees as price converges within about 30 ticks.** It is falsified if net P&L is not above a do-nothing control, if entering later does not reduce it, or if a flipped news sign still makes money.

**Rule:** buy while `ask ≤ fair × (1 − edge)` (good news) or sell while `bid ≥ fair × (1 + edge)` (bad news), where `edge` = 2 × taker fee + a buffer and is also the IOC limit. Exit at the target or after 30 ticks.

**Disclosure:** the Week 2 and Week 4 tapes shaped this hypothesis (`exploration/`): quotes were pulled and the touch gapped past fair value within 1–2 ticks, and plain momentum lost after costs on Week 2. The Week 5 and Week 6 books were never used to choose anything.

## 2. Data and method

- **Simulator:** the production matching engine (`HeadlessArena`) with the week 6 calendar and house bots, running the real `team/trader.py` seat unchanged. Three defaults that flatter the strategy were corrected: fees 40/0 bps (default 15/10), halts 300 ticks (default 0.05 s), and the same prints for every variant of a seed.
- **P&L:** excess over a do-nothing control in the same session, net of fees and carry. A seat that does not trade scores exactly $0.
- **No fitting on evaluation data:** 18 parameter sets were compared on 12 training seeds (1000–1011). The best (buffer 0 bps, target 1.0, entry window 15 ticks) was frozen and run once on 40 held-out seeds (2000–2039). Training means ran from +$682 to +$1,096, with the top sets within noise.

## 3. Results (held-out seeds, 1 tick of latency, 40 bps)

| Seats | Mean P&L per session | SD | t | Sessions > 0 | Trades | Hit rate | Worst drawdown | Fees per session |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| One | **+$1,195** | $942 | 8.0 | 40/40 | 151 | 91% | $2,094 | $836 |
| Two (run live) | +$1,727 | $1,176 | 9.3 | 39/40 | 316 | 93% | $2,172 | $1,379 |

No liquidations or rejects. Naive baselines in `SimSession` (5,000 ticks, 40 bps, 20 seeds): momentum −$227, mean reversion −$866; EventGap makes 0 fills there because that flow has no prints.

## 4. Look-ahead check

The signal reads only the pre-print mark and the current touch. Shifting the reaction on the same seeds:

| Entry | Instant | **+1 (base)** | +2 | +3 | +5 | +8 | +15 | Told 5 early (leak) | Sign flipped (placebo) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Mean P&L | +$1,513 | **+$1,195** | +$981 | +$799 | +$533 | +$99 | $0 | +$3,474 | −$4,706 |

The edge decays steadily with delay, a deliberate leak is detected (+$3,474), and the placebo loses.

## 5. Where the simulation is too kind

| Test (one seat, 40 bps) | Mean P&L |
|---|---|
| Simulator, market makers pull quotes for 8 ticks after a print, 1 tick late | **−$1** (t = −0.1) |
| Real Week 4 book, latency 0 / 1 / 2 / 3 ticks | +$1,139 / +$420 / +$152 / +$121 |
| **Real Week 5 book (unseen), latency 1 / 2 / 3 / 5** | **−$219 / −$973 / −$2,109 / −$2,429** |

The simulator's market makers requote only every 5 ticks, which leaves stale quotes to take; real ones pull them at once. The replay has no impact or racing but sees only each tick's last snapshot. **Forecast, written before the live run:** first $0 to +$500; after the Week 5 replay, "a loss more likely than a gain."

## 6. Live comparison: Week 6, Fri Nov 6

| Source (same session, prints only, two seats throughout) | P&L |
|---|---|
| Simulator, two seats | +$1,727 |
| Replay of the signal on Friday's real book, two seats sharing its depth, latency 0 / 1 / 2 | +$972 / −$126 / −$472 |
| **Live, both seats** | **−$110** (gross +$1,935, fees $2,045) |
| Live, everything the trader seats traded (`results/live_week06.json`) | −$1,060: the IPO −$1,028, signal and probes −$32 |

The edge did not survive costs live, as the replays predicted. The gap from the simulator has four sources:

1. **Fees.** 40 bps each way consumed the entire gross: $2,045 of fees on +$1,935 gross.
2. **Latency.** Our first fills came 0–10 ticks after each print. On Friday's book the replay is profitable only at latency 0.
3. **Convergence held for earnings but not the econ print.** INTC and MSFT reached about 97% of the announced move by t+60. On the econ print AAPL and AMZN were back to 0.51 and 0.49 of it at t+30, the time stop, so short entries were covered at a loss (AMZN −$583). MSFT earnings, the cleanest case, made +$875, beating its replay (+$346).
4. **Our own impact, and a second seat that adds nothing on a real book.** In the simulator a second seat adds 44% (+$1,727 vs +$1,195), because its market makers leave depth stale. On Friday's real book the first seat takes all the stale depth before reaching its cap, so the two-seat replay equals the one-seat replay and seat 2 gets no fills. Live, the seats chased the same MSFT quotes (one bought 206 shares, the other 80), and our average entry rose from $425.8 to $428.2 across successive orders.

**Verdict:** the mechanism is real (look-ahead checks; MSFT), but net of 40 bps fees, real latency and real liquidity, the edge is about zero.

## 7. Replay evidence (Part A; played through `scripts/replay_session.py`: `results/replay_week0{5,6}_wire.txt`, plus `replay_week0{5,6}.txt`)

| Session | No liquidation | Position limit | Activity after `TRADING_RESUMED` | Other |
|---|---|---|---|---|
| 5, Risk (recording timestamped Thu Oct 29) | No `LIQUIDATION` events | Largest team change from fills 105 TSLA; trader_1 peaked at 118 NFLX (limit 800) | trader_1 filled 0–4 ticks after the 6 resumes during trading | NFLX +7.6%: bought 118 within 4 ticks, held through the halt, **+$446** net |
| 6, Tournament (Nov 6) | No `LIQUIDATION` events | Largest change from fills 212 MSFT, plus 342 VLTA allocated (limit 1000) | A trader seat filled 0–20 ticks after all 7 resumes | VLTA bid: each seat sold 171 more than it bought, matching a pro-rata allocation (≈169 of a 400 bid at 3,500/8,282). VLTA broke issue ($34 → $31.1), and the 4% stop exited it. | Trader fills run from the open to t+1245.

## 8. Plot

`equity_curve.png`: the backtest's held-out seeds against the live trader seats on Friday (trades only, including the IPO and probes). (In folder)
