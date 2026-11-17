# RISK: STT_11 risk layer (Phase 3)
**Team STT_11:** Megan Maloney, Brett Suazo, Hannah Kong
**Code:** `team/risk.py` contains every guard. The trader (`MyTrader.gate`), broker (`BudgetedBrokerBot._guard`) and venue (`MyExchange.accept_order`) ask it before any order leaves. **Tests:** `tests/test_risk_guards.py` (35 tests, at least one per guard; output in `results/test_output.txt`, full suite 314 passed). **Under stress:** `stress_session.py`. **Session 6 replay:** `replay_risk.py`. Per-guard evidence with log excerpts is in `RISK_EVIDENCE.md`.

## Limits and thresholds (checked before an order is sent; a breaching order is cut or dropped)
| Guard | Trader seat ($100k) | Broker | Action | Why this number |
|---|---|---|---|---|
| Position per symbol | (limit 1000 − 20 − broker 150) ÷ signal seats = 830; team total checked across seats | ±150 shares | shrink order | The venue limit is per team, so the broker's maximum is reserved and a quote fill can't push the team over. |
| Gross notional | ≤ 1.5 × equity | ≤ 1.5 × equity | shrink order | One full-cap print trade (live peak 212 MSFT ≈ 0.9×) fits; three coinciding prints don't. Stress peak was 0.60×. |
| Net notional | ≤ 1.0 × equity | ≤ 0.5 × equity | shrink order | The worst econ print (3%) against a full net book costs 3%, exactly the soft stop. The broker's inventory comes from its flow rather than its choice, so it gets half the room. |
| Position stop | −4% of entry and ≥ $150 | −2% and ≥ $150 | close it; no new exposure in the symbol for 60 ticks; ends the strategy trading it | Trader: the signal trades with an announced move, and the worst seen against it (econ-print reversion) is about 1.5%, so −4% means the thesis broke. It matches the listing stop and sits under the 8% LULD band. Broker: 2% is over 100 of its 15 bps half-spreads. The $150 floor ignores noise on 5-share probes. The 60-tick cool-down outlasts the signal's 30-tick horizon. |
| Session soft stop | −3% of capital | −3% | reduce-only for the rest of the session | 1.4× the worst backtest drawdown ($2,172) and about 3× the live Nov 6 loss ($1,060), so a normal bad day never trips it. |
| Session hard stop | −5% | −5% | **kill switch + flatten** | About 2× anything seen in backtest or live, so past it the bot is not behaving as tested. It sits far above the venue's 60% reduce-only line and 40% liquidation line, so the seat survives the season. |
| Reject storm | ≥ 10 rejects in 30 ticks and ≥ 50% of orders sent | ≥ 20 | kill switch for 60 ticks (no flatten) | Live week 7: 0 rejects in 3,951 broker messages and 38 trader orders; stress runs: 0. Ten means a loop is misfiring. Benign codes (`ORDER_NOT_FOUND`, `STP_CANCEL`) don't count. |
| Reconnects | ≥ 3 reconnects in 300 ticks | same | kill switch for 120 ticks | After a reconnect the portfolio comes from the leaderboard, and a flaky link means trading on a stale picture. |
| Stale feed | no market data for 15 ticks | no leaderboard for 15 ticks | kill switch until data returns | Snapshots arrive every 0.5 s (also during halts) and the leaderboard every 2 s, so 15 ticks of silence means a dead link. |
| **Manual kill** | `python scripts/kill_switch.py on [--flatten] [--seat ID]` | same | stop sending, optionally flatten | Writes `Phase2/state/KILL[.seat]`, which every seat polls each tick. `off` releases it. Automatic loss trips clear at the next session; a manual trip clears only when a human removes it. |

There is one kill switch per seat with several possible causes. While any cause is on, `check()` refuses new exposure. With `flatten`, only closing orders pass, one IOC per tick, largest position first, skipping halted symbols until they resume. Unlisted IPO allocations are valued at the offer price, and a tick on which any held position has no price at all is not judged (see the bug below).

**Broker.** Quotes skew against inventory and reach a full half-spread at 100 shares, so a long book shades both quotes down. Width is the larger of 30 bps and 3× rolling volatility, ×2 after toxic fills (markout < −15 bps), ×3 for a new listing, capped at 300 bps. Quotes are pulled for 30 s after a SHOCK or print, and inventory is flattened before earnings.
**Exchange (venue policy, `VenueGuard`).** Each seat gets a token bucket of 30 orders/s with a burst of 60, and 3 breaches in 10 s means 5 s in a penalty box. That is over 6× what our own bots send (5/s and 2/s), so only a runaway loop hits it. Single orders over $250k notional, or more than 10% through the last trade, are refused as fat fingers. House bots are exempt. Tested: 10 orders/s for a minute never rejected; a 70-order burst is boxed while other teams are unaffected.

## On a halt
**Our seats.** The halt tracker learns of a halt from `TRADING_HALT` and `MARKET_HALT`, from a `SYMBOL_HALTED` reject (one we missed), or from a timeout (a resume we missed). Nothing is sent to a halted symbol, so there are no reject storms. The broker cancels its quotes at once, since stale quotes would be free money for whoever lifts them first. **Re-entry is deliberate.** After `TRADING_RESUMED`, new exposure waits at least 3 ticks *and* for a two-sided book under 100 bps wide; exits may go at once. The broker waits 5 s.
**Our venue.** Orders in a halted symbol are refused with a clear reason. For 10 s after it reopens, the collar tightens to 3% and market orders are refused, so the first prints come from priced orders and not a sweep of a stale book.

## Evidence it works
**Stress (real engine, our seats, week 6 calendar plus unscheduled AMD −9%, NFLX +7% and market −6% shocks, plus a manual kill mid-session), 5 seeds:** 0 crashes, 0 liquidations, **0 rejects**, **0 orders into 69 halts**. Peak position 400 (the IPO allocation) against a limit of 1,000; peak gross 0.60×. Same seeds with every guard off: P&L **+$5,149** against **+$7,718** with the guards on, so the layer pays for itself and doesn't strangle the strategy. With the session stops tightened (`--limits tight`), the hard stop → kill → flatten chain fires on the market shock.
**Bugs the stress run found and fixed (now regression-tested):**
1. A priced but unlisted IPO had no mark, read as a −$9.9k session, and tripped the hard stop. This would have fired at LUNA's pricing.
2. A stop exit was priced off a stale bid outside the LULD band after a −9% gap, causing a PRICE_BAND reject.
3. After a stop, the signal plan kept trying to buy back the position the stop had sold (41 blocked orders).

## Session 6 replay (Fri Nov 6, `results/replay_session6_risk.txt`, from `scripts/replay_session.py`)
The tape went through the replay server, with all 47,833 captured messages identical to the recording (only the first 28, sent before the listener connected at t0, were missed).
- **Live (the week 6 bot, before this phase).** 22 scenario events: 5 halts and resumes, the INTC and MSFT earnings, the econ print, the VLTA IPO. No liquidation. The VLTA bookbuild got a bid with an allocation of 171 per seat. Largest positions were 192 MSFT and 171 VLTA (limit 1,000). Two fills landed in the halt's first tick (IOCs already in flight). After INTC and TSLA resumed, the live bot traded at **+0 ticks**, the behaviour this phase removes.
- **Today's risk layer on the same tape.** No session stop on either seat; the worst session P&L was −$722. At IPO pricing the allocation is valued at the $34 offer, so the session P&L reads −$103; at the old $0 valuation it would have read −$5.9k. The resume gate would have held new exposure for **+3 to +6 ticks** on the real post-halt books. The VLTA position stop fires at t956 at −4.1% (it broke issue at $34), the same exit the 4% listing stop took live.

**Limits.** The broker is not in the stress simulator (it needs a socket), so its evidence is the unit tests plus the live week 7 log. Nov 6 itself ran the pre-Phase-3 bot; its replay shows what this layer would have done on that tape, not a live run of it.
