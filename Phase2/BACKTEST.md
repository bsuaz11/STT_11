# BACKTEST: EventGap, a post-print fair-value signal (Trader role)

**Team STT_11:** Megan Maloney, Brett Suazo, Hannah Kong

**Reproduce:** `PYTHONPATH=engine:. python Phase2/backtest.py` (about 15 min), `--verify` to check the stored numbers, `Phase2/tape_replay.py` for the recorded books. Run on 2026-11-04.

## Hypothesis

A `CALENDAR_EVENT` publishes a signed move, so everyone immediately knows the new fair value: `pre_mark × (1 + pct)`. Resting liquidity does not reprice that instantly. **Claim: orders still on the wrong side of the new fair value by more than the round-trip cost are stale, and taking them earns a net profit as price converges to fair value within about 30 ticks.**

It is falsified if P&L net of fees is not above a do-nothing control, if entering later does not reduce it, or if it survives a flipped news sign.

**Rule.** Good news: buy while `ask ≤ fair × (1 − edge)`. Bad news: sell while `bid ≥ fair × (1 + edge)`. `edge` is 2 × taker fee + a buffer, and it is also the IOC's limit, so an order cannot pay through it. Exit when the touch reaches the target or at a 30-tick time stop.

**Disclosure.** This hypothesis was not formed blind. An event study of the Week 2 and Week 4 tapes showed quotes being pulled and the touch gapping past fair value within 1–2 ticks, which replaced our earlier plan of riding the ramp (`exploration/event_study.py`). A grid on Week 2 had already shown that plain "big move continues" momentum loses after costs (`exploration/momentum_grid.py`). So those tapes are a realism check here, not clean out-of-sample data. The Week 5 tape arrived after the parameters were frozen, so it is the first clean test on a real book.

## Data and method

- **Simulator:** the production `ExchangeServer` matching engine on a virtual clock (`HeadlessArena`) with the week 6 calendar and house bots, running the real `team/trader.py` seat, so backtest and live share one code path. Three defaults were corrected because they flatter the strategy: fees set to the live 40/0 bps (engine default 15/10), halts set to 300 ticks (simulator default 0.05 s), and prints drawn per seed so every variant of a seed faces identical events.
- **P&L:** excess over a do-nothing control in the same session, net of fees and carry. A seat that does not trade scores exactly $0.
- **Avoiding fitting on evaluation ticks:** 18 parameter sets were compared on 12 training seeds (1000–1011). The best (`buffer 0 bps, target 1.0, entry window 15`) was frozen and evaluated once on 40 held-out seeds (2000–2039). Training means ranged from +$682 to +$1,096, and the top sets were within noise of each other. Smoke tests touched seeds 1000–1001 and 2000–2001 earlier with fixed parameters; nothing was changed because of them.
- **Reproducibility:** all 40 held-out seeds reproduce to the cent (`results/verify.txt`). Each session needs its own process because engine globals leak between sessions.

## Results (held-out seeds, one seat, 1 tick of latency, 40 bps taker)

| Mean P&L per session | SD across seeds | t | Median | Range | Sessions > 0 | Trades | Hit rate | Worst drawdown | Fees per session |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **+$1,195** | $942 | 8.0 | +$918 | +$87 to +$3,301 | 40/40 | 151 | 91% | $2,094 | $836 |

No liquidations, no rejects, peak position 255 shares, 2.8 halts per session. About 70% of the profit comes from the econ print, about 29% from MSFT earnings, and about 1% from INTC (a $22 stock, so little notional). Two seats earn +$1,727 (t = 9.3), so both trader seats run the signal live.

Naive baselines in `SimSession` (5,000 ticks, 40 bps, 20 seeds): momentum **−$227** (SD $36), mean reversion **−$866** (SD $40). EventGap makes 0 fills there, as it should: that flow has no prints.

## Look-ahead check

The signal reads only the pre-print mark and the current touch, and the module has no access to later ticks. To test this empirically, the reaction was shifted in time on the same seeds:

| Reaction | Mean P&L | t | | Control | Mean P&L | t |
|---|---:|---:|---|---|---:|---:|
| Instant | +$1,513 | 8.3 | | **Told 5 ticks early (deliberate leak)** | +$3,474 | 18.5 |
| **1 tick late (baseline)** | **+$1,195** | 8.0 | | Told 1 tick early (deliberate leak) | +$1,735 | 10.0 |
| 2 ticks late | +$981 | 7.2 | | **News sign flipped (placebo)** | **−$4,706** | −20.9 |
| 3 ticks late | +$799 | 6.7 | | | | |
| 5 ticks late | +$533 | 7.2 | | | | |
| 8 ticks late | +$99 | 2.5 | | | | |
| 15 ticks late | $0 (no trades) | | | | | |

The edge decays steadily with delay, as a real edge should. The leaks show the harness would detect look-ahead: a real leak earns far more than the baseline, and the baseline shows no such inflation. The placebo loses heavily.

## Where the simulation is too kind

The simulator's market makers requote only every 5 ticks, which leaves stale quotes to take. The real tapes show quotes pulled at once, so two further tests target that gap:

| Test | Latency | Mean P&L |
|---|---:|---:|
| Simulator, market makers pull quotes for 8 ticks after a print | 1 tick | **−$1** (t = −0.1) |
| Same | instant | +$547 (t = 5.0) |
| Real Week 4 book, 40 bps (Week 4 was charged 30: +$751 at 1 tick) | 1 tick | +$420 |
| Real Week 4 book, 40 bps, by latency | 0 / 2 / 3 / 5 ticks | +$1,139 / +$152 / +$121 / +$5 |
| Real Week 2 book | any | $0: no trades (shorts locked, no asks on the good-news print) |
| **Real Week 5 book, 40 bps (unseen, frozen parameters)** | **1 / 2 / 3 / 5 ticks** | **−$219 / −$973 / −$2,109 / −$2,429** |
| Real Week 5 book at the 30 bps it was charged | 1 / 2 / 3 / 5 ticks | +$170 / −$558 / −$1,835 / −$2,447 |

The replay has no impact or racing, but it sees only each tick's last snapshot, so it can also miss prices a live order would get. Week 4 is about one independent observation, since most of its plans come from a single econ print.

**The clean test does not support the edge at today's fee.** On Week 5 the signal loses at 40 bps at every latency tested. The largest loss is the NFLX print: price reached only $881 of the $912 fair value during the halt, so convergence partly failed, and the first exit at resume hit a gapped bid ($840 against an $881 mark). Our Oct 29 bot took 2–3 ticks to its first fill, though the ask side was also empty at the print, so the 2–3 tick rows are the realistic ones. Most of the simulated profit depends on stale quotes that real market makers largely remove within a tick or two. We did not retune on this tape.

**Forecast, written before the live run:** first estimate: roughly $0 to +$500, far below the simulated +$1,727. After the Week 5 replay, we expect a loss to be more likely than a gain, up to a few thousand dollars across two seats (about 2% of their capital). The IPO and probe trades are outside this forecast.

## Live comparison

*Pending the Week 6 block on Fri Nov 6, 15:30 CT.* To be filled from `Phase2/live_compare.py`, which reports live P&L per print beside a replay of the signal on that same day's book, so the gap separates what was available from what we captured: fees, queue position, latency and our own impact.

## Replay evidence

**Session 5, Risk scenario** (recording timestamps: Thu Oct 29, 17:10–18:00 CT). This session ran the earlier bot, before EventGap. Played through `scripts/replay_session.py` (`results/replay_week05_wire.txt`, `results/replay_week05.txt`):

| Check | What the recording shows | Result |
|---|---|---|
| No forced liquidation | No `LIQUIDATION` event for any seat | Pass |
| Position limit (800) | Largest net team change from our fills: 105 TSLA, and trader_1 reached 118 NFLX (market tape, no leaderboard, so about 20 granted shares are excluded) | Pass |
| Activity after `TRADING_RESUMED` | Trader_1 filled in the symbol within 0–4 ticks of the 6 resumes during the session, and within 3 and 18 ticks of the 2 just before the first print | Pass |

**Through the events:** NFLX +7.6%: bought 118 at about $859 within 4 ticks, held through the 300 s halt, sold at $866–$875 on resume, **+$446 net of $638 fees**. AMD −8.3%: no position taken; once a bid appeared it was already below fair value. Econ −3.0%: only 5-share post-halt trades. Trader_2 has no fills in the recording, consistent with mean reversion never firing, and that role has been removed.

**Session 6:** pending Friday's recording.

## Plot

`equity_curve.png`: excess P&L for each held-out seed. The live curve is overlaid with `plot_equity.py --live` after Friday.
