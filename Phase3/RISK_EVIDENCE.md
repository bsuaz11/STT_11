# RISK_EVIDENCE: one item per guard
**Team STT_11:** Megan Maloney, Brett Suazo, Hannah Kong

Each guard has a unit test in `tests/test_risk_guards.py` (35 passed, `results/test_output.txt`). Where it also fired in an engine session, the annotated excerpt below comes from `results/stress_seed*.log`. Those runs use our real trader seats on the production matching engine (`stress_session.py`; ticks are `tNNNNN`, and both trader seats write to one log). Reproduce with:

```
PYTHONPATH=engine:. pytest tests/test_risk_guards.py -v
PYTHONPATH=engine:. python Phase3/stress_session.py --seed 11 --limits tight
PYTHONPATH=engine:. python Phase3/replay_risk.py sessions/session_week06_20261106_214023_market.jsonl.gz
```

## 1. Hard limits (checked before the order is sent)
| Guard | Test | Shows |
|---|---|---|
| Position per symbol | `test_limit_max_position_per_symbol_shrinks_the_order_before_it_is_sent` | 500 asked with 400 held → 430, the seat cap of 830 |
| Gross notional 1.5× | `test_limit_gross_notional_caps_new_exposure` | $139.5k gross on $100k equity leaves room for 105 AMD |
| Net notional 1.0× / 0.5× | `test_limit_net_notional_caps_a_one_way_book` | long book: buys capped, sells free; broker cap tighter |
| Reducing is never blocked | `test_limit_never_blocks_reducing_a_position` | 2.4× gross: buy room 0, sell room ≥ the position |
| Never reaches the wire | `test_limit_breaching_order_is_never_sent_on_the_wire` | through `TakerBot._place_order`: 500 → 300 sent; then nothing sent |
| Broker gross cap | `test_broker_gross_cap_cuts_the_quote` | quote dropped at $400k gross on $300k |

In the engine: across 5 seeds, peak gross was 0.60× (cap 1.5×) and peak position 400 (limit 1,000). The cap cut real orders:
```
t00966  INFO  team.trader  Risk: sell MSFT 500 -> 393 (notional cap (room 393))     ← stress_seed23
```

## 2. Stop-loss
**Per position** (`test_position_stop_flattens_the_loser_and_blocks_reentry`, `test_position_stop_ignores_small_noise_below_the_dollar_floor`, `test_broker_position_stop_closes_the_inventory`). Seed 11: an unscheduled AMD −9% gap at t180 → stop → exit priced off the mark, inside the band. AMD halts in the same tick, so the exit waits and goes after the resume:
```
t00180  ERROR    team.risk    POSITION STOP AMD: +20 @ entry 164.96, mark 150.49, loss $289.43 — flattening, no new exposure for 60 ticks
t00180  WARNING  team.trader  POSITION STOP: sell 20 AMD @ 150.19
t00486  WARNING  team.trader  POSITION STOP: sell 20 AMD @ 148.58                 ← after the t180–t480 halt
```
A stop ends whatever strategy was trading the symbol, so it can't buy back what the stop sold (`test_a_position_stop_ends_the_signal_plan_instead_of_fighting_it`):
```
t00968  ERROR    team.risk    POSITION STOP TSLA: +20 @ entry 279.89, mark 268.28, loss $232.25 — flattening, …
t00971  WARNING  team.trader  POSITION STOP: sell 20 TSLA @ 266.19
t00971  WARNING  team.trader  POSITION STOP TSLA: ended the signal plan          ← stress_seed23
```
**Session soft stop** (`test_session_soft_stop_goes_reduce_only`). At the real 3% threshold, on the unscheduled market −6% shock:
```
t01100  ERROR  team.risk  SESSION SOFT STOP: P&L -3551.38 ≤ −3% of capital 100000 — reduce-only   ← stress_seed11
```
**Session hard stop → kill + flatten** (`test_session_hard_stop_trips_the_kill_switch_and_flattens`). This run uses `--limits tight` (soft 1%, hard 2%) so the chain fires on that same shock:
```
t01100  ERROR    team.risk    SESSION HARD STOP: P&L -3551.38 ≤ −2% of capital 100000 — kill + flatten
t01100  ERROR    team.risk    KILL SWITCH ON [session_loss] seat=stt_11_trader_1 flatten=True until reset
t01100  WARNING  team.trader  KILL SWITCH flatten: sell 20 NFLX @ 702.00
t01101  WARNING  exchange.server MARKET HALT L1 (900 s) …                        ← nothing more sent: all halted
```

## 3. Kill switch
| Trigger | Test | Behaviour |
|---|---|---|
| Human, mid-session (`scripts/kill_switch.py`) | `test_kill_switch_manual_file_stops_orders_mid_session_and_releases`, `…_manual_flatten_closes_positions_one_seat_only`, `…_script_writes_and_removes_the_file` | stops, flattens one seat only, releases |
| Session loss | (§2 hard stop) | kill + flatten, latched until the next session |
| Reject storm | `test_kill_switch_auto_reject_storm_cools_off` | 9 rejects: no trip; benign codes ignored; 10 of 12 sent: trip; 60 ticks later: released |
| Reconnects | `test_kill_switch_auto_reconnect_storm` | the 3rd reconnect in 300 ticks trips it, with no flatten |
| Stale feed | `test_kill_switch_auto_stale_feed_and_recovery` | 16 silent ticks trip it; data releases it |
| Several causes | `test_kill_switch_holds_several_causes_and_logs_each`, `test_new_session_resets_automatic_trips_but_not_a_manual_kill` | |
| Broker | `test_broker_kill_switch_pulls_every_quote_and_flattens` | cancels every quote, IOC flatten, new quotes refused |

The manual kill in an engine session (every stress seed; this one is seed 11). It flattens one position per tick and holds NFLX, halted t420–t720, until it resumes:
```
t00700  WARNING  team.stress  HUMAN: dropped KILL.stt_11_trader_2 (flatten)
t00701  ERROR    team.risk    KILL SWITCH ON [manual] seat=stt_11_trader_2 flatten=True until reset
t00701  WARNING  team.trader  KILL SWITCH flatten: sell 20 TSLA @ 280.39
   …  AAPL t702, AMZN t703, GOOGL t704, NVDA t705, META t706, INTC t707
t00726  WARNING  team.trader  KILL SWITCH flatten: sell 20 NFLX @ 763.84            ← first ticks after its halt
t00760  WARNING  team.stress  HUMAN: removed KILL.stt_11_trader_2
t00761  WARNING  team.risk    KILL SWITCH released [manual] seat=stt_11_trader_2
```

## 4. Halts and shocks
| Guard | Test |
|---|---|
| Nothing sent while halted (no reject spam) | `test_halt_no_orders_while_halted_so_no_reject_spam` (20 ticks: 0 sent, 0 rejects) |
| Deliberate re-entry | `test_halt_reentry_waits_for_settle_and_a_two_sided_book` (first tick: no; one-sided: no; 600 bps wide: no; settled: yes; exits always) |
| Shock | `test_shock_broker_pulls_quotes_and_trader_reads_it_as_a_print` |
| Venue halt policy | `test_venue_halt_policy_refuses_then_reopens_with_a_tight_collar`, `test_venue_market_halt_blocks_everything_until_resumed` |

Engine (seed 7). AMD resumes at t480; new exposure waits for the settle and a 50 bps two-sided book:
```
t00480  INFO  team.trader  RESUMED AMD — re-entering only once the book settles
t00480  INFO  team.risk    RESUME AMD: no new exposure for 3 ticks, then only into a two-sided book
t00486  INFO  team.risk    RESUME AMD: book settled (50 bps) — re-entering
```
Across 5 seeds and 69 halts, the seats sent **0 orders into a halted symbol** and drew **0 rejects**.

## 5. Role specifics
Broker: `test_broker_skew_shades_quotes_down_when_long_and_up_when_short`, `test_broker_widens_on_volatility_and_on_toxic_flow`. Exchange: `test_venue_normal_trader_is_never_rate_limited` (10 orders/s for 60 s, never rejected), `test_venue_abusive_flow_is_rate_limited_then_boxed`, `test_venue_house_bots_are_exempt`, `test_venue_fat_finger_collar_and_notional_cap`. Live week 7 (Nov 13, `Phase2/logs/20261113_123605`): the broker sent 3,951 messages and the trader 38 orders in the graded block, with **0 RATE_LIMITED and 0 rejects**.

## 6. Bugs the stress run found (each now has a regression test)
1. **IPO pricing tripped the hard stop** (`test_ipo_allocation_before_listing_is_not_a_session_loss`, `test_a_position_with_no_price_at_all_skips_the_session_check`). From the first stress run, before the fix:
   ```
   t00432  INFO   team.trader  Event: IPO_PRICED | VLTA priced at $34.00 …
   t00432  ERROR  team.risk    SESSION HARD STOP: P&L -9925.63 ≤ −5% of capital 100000 — kill + flatten
   ```
   The allocation was paid for but had no mark until listing, so it was valued at $0. It is now valued at the offer. On the Nov 6 tape, the session P&L at pricing reads −$103.
2. **PRICE_BAND reject on a stop after a gap** (`test_stop_exit_after_a_gap_is_priced_inside_the_luld_band`). The exit was priced off a stale 164.65 bid with the mark at 150.45. It is now priced off the worse of touch and mark and clamped into both band edges. Rejects across all runs went from 1 per seat to 0.
3. **Stop and strategy fighting** (§2): 41 blocked buy-backs per session became 0.
