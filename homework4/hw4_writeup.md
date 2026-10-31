## 1. Store it twice

**Rows and clocks.** Every row in both stores carries `week`, `seq`, `tick`, `t` (tick minus the scenario's t0, so `t = 400` is "t400"; t0 is found by matching each CALENDAR announcement with the CALENDAR_EVENT that fired, on 2 events in Week 2 and 3 events in Week 4) and the venue's wall clock:`ts` 

**SQLite**  There is one table per message type with `NOT NULL` columns where the venue always sends a value. The primary key is `(week, seq)` on every table except for `leaderboard`, whose key is `(week, seq, team_id)` because one message carries one row per team. All tables are `WITHOUT ROWID`, so each table is its primary key B-tree. The depth is normalized into `book_level(week, seq, side, lvl, px, qty)`, with keys on `(week, seq, side, lvl)`. A `load_log(week PK)` table refuses a week that is already loaded.


| index | columns | usage |
|---|---|---|
| `book_wst` | `book_snapshot(week, symbol, t, seq)` | Every as-of lookup (Q1, Q2, Q4a): "last book of SYMBOL at tick ≤ T" and "first tick ≥ T" are range seeks on `(week, symbol, t)`. The trailing `seq` picks the end-of-tick snapshot from the index alone (the plan says `USING COVERING INDEX book_wst`). |
| `trade_taker`, `trade_maker` | `trade_execution(taker_id \| maker_id, week, seq)` | "My fills" (Q2a, Q2b, Q4b). A seat can be either side, so there's one index per role and the plan is a `MULTI-INDEX OR` instead of a scan. |
| `ack_team` | `order_ack(team_id, week, seq)` | My orders: reaction time on the clock page. |
| `board_team` | `leaderboard(team_id, week, t)` | My net worth at a tick. |
| *(none)* | `session_event`, `book_level`, cancels, rejects | `session_event` is 7–19 rows a week, which is one page. `book_level`'s primary key already is the index the point lookup needs. Nothing filters cancels or rejects. |

**Parquet** There is one dataset per message type, partitioned `week=N/symbol=S/` (`week=N/` only for tables with no symbol), compressed with zstd. Rows are sorted by `(tick, seq)` and written in row groups of at most 4,096 rows, so the `t`/`seq` min/max statistics in each file cover disjoint tick ranges. Depth is four `list<double>` columns on `book_snapshot`. File names carry the week, and the loader refuses a week whose `week=N` folders exist.


**Appending a week** Week 4 was added to the existing Week 2 stores with the two commands above. So it's one new transaction in SQLite so that`load_log` now has weeks 2 and 4, and there are new `week=4` folders in Parquet. R

### Storage table

| week | JSONL in (gzip on disk) | SQLite, no index | SQLite, 5 indexes | Parquet (zstd) | SQLite load | index build | Parquet convert |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | 265.0 MB (54.4 MB) | 56.1 MB | 62.5 MB | 7.0 MB | 14.6 s | 0.12 s | 8.5 s |
| 4 *(market tape)* | 19.6 MB (1.2 MB) | 22.9 MB | 23.8 MB | 1.7 MB | 2.1 s | 0.03 s | 1.1 s |
| **both, as stored** | 284.6 MB | | **86.4 MB** | **8.7 MB** | | | |

Each week is loaded alone into a scratch file for its row; the "as stored" row is the real two-week stores. Most of each load is parsing the JSON (9.6–10.5 s for Week 2).

| rows | week 2 | week 4 |
|---|---:|---:|
| book_snapshot | 55,375 | 36,096 |
| book_level (SQLite only) | 862,067 | 654,169 |
| trade_execution | 4,511 | 4,506 |
| order_ack / order_cancel / order_reject | 34,881 / 28,291 / 21,294 | not on the market tape |
| session_event | 7 | 19 |
| leaderboard (one row per team per board) | 120,132 | not on the market tape |

---

## 2. The same questions in both stores, timed

- The clock rules are the same in both logical and real clocks
- Windows are in ticks, never seconds.
* **End-of-tick book.** A symbol gets up to 17 snapshots in one tick. "The book at tick T" always means the last snapshot of that tick by `seq`. An as-of match on `t` alone would let each engine keep a different snapshot.
* **Q1 event windows** (backward). pre = the end-of-tick book at the last tick ≤ ev_t − 1, strictly before the event's tick, because the mid already moves inside it. +k = the end-of-tick book at the last tick ≤ ev_t + k. This runs over every CALENDAR_EVENT x affected name in both weeks.
* **Q2a book just before each of my fills** (backward: `b.seq < f.seq`): the last book of that symbol written before the fill line, even inside the same tick. A snapshot written later in the same tick already includes my fill, so it isn't what I paid against.
* **Q2b markout** (forward): the end-of-tick book of the first tick ≥ f.t + k, k = 5 and 60. Post-trade only. A backtest decision that used it would be look-ahead. Q2c is the same join over every fill on both tapes (9,017), to find where SQLite stops winning.
* **Q3** full scan: mean spread and touch size by symbol over both weeks. Q4a point lookup: AAPL's full book at the end of t400, Week 2. Q4b*point lookup: my net position at the end of t1400, Week 4.

| query | rows | SQLite best | SQLite 1st | DuckDB/Parquet best | DuckDB 1st | faster | same |

| Q1 event windows | 26 | 0.18 ms | 7.88 ms | 20.13 ms | 43.42 ms | SQLite 109.3x | yes |
| Q2a my fills: prevailing book (backward) | 38 | 0.20 ms | 6.73 ms | 19.06 ms | 5,267 ms | SQLite 97.4x | yes |
| Q2b my fills: markout +5/+60 (forward) | 38 | 0.24 ms | 4.14 ms | 23.05 ms | 35.07 ms | SQLite 95.1x | yes |
| Q2c all fills: markout +60 (forward) | 1 | 34.93 ms | 88.39 ms | 21.73 ms | 27.26 ms | DuckDB 1.6x | yes |
| Q3 full scan: spread & touch depth by symbol | 12 | 59.43 ms | 62.25 ms | 8.71 ms | 10.56 ms | DuckDB 6.8x | yes |
| Q4a point: AAPL book at end of t400, week 2 | 10 | 0.02 ms | 0.45 ms | 8.89 ms | 15.98 ms | SQLite 499.6x | yes |
| Q4b point: my position at end of t1400 (--point-week) | 2 | 0.92 ms | 2.43 ms | 6.37 ms | 11.70 ms | SQLite 6. 9x | yes |

Best of 7 runs, warm OS cache; "1st" is the first run. The "faster" ratios are computed from the unrounded times. DuckDB's 5,267 ms first run on Q2a was a one-off, most likely cold-start cost: its best of 7 was 19.06 ms.

Answers:
Q1: Week 2 AAPL earnings (+6.20 %) was +2.69 / +7.74 / +8.64 % at +5 / +15 / +60. Week 4 AMZN earnings (−7.91 %) was already −6.64 % at +5, then −6.51 / −5.84 %. GOOGL (−5.66 %) was −4.77 / −3.30 / −1.41 %. The Week 4 econ print (−2.49 %) moved the 12 names −0.31 % to −2.02 % at +5 and −3.22 % (ORCA) to +1.03 % (INTC) at +60.

Q2a/Q2b: my 38 fills (16 by trader_2 in Week 2, 22 by trader_1 in Week 4) were all taker fills. The 35 that met a two-sided book paid +2.93 bp over the touch mid. Markout was +86.2 bp at +5 ticks and +38.9 bp at +60 (36 fills had a book 60 ticks later), worth $762 and $1,112: the shock trades sold ahead of the drop.

Q4a: at the end of t400 AAPL had 10 bid levels and no asks. The earnings move had already taken them out. Q4b: after 1,400 ticks of Week 4, trader_1 had net fills of GOOGL −16 and MSFT 0.
Which store won which query, and why

SQLite won every selective question, by 6.9 times to 500 times. DuckDB over Parquet won the full scan, by 6.8 times. On my laptop it also won the as-of join when I asked it about every fill on both tapes instead of only mine, by 1.6 times.

Both results come from how many bytes each store has to touch. AAPL's book at t400 took SQLite 0.02 ms: one seek down the book_wst B-tree to (2, 'AAPL', 400) gives the seq, and one range of book_level's primary key returns the 10 levels, a few 4 KB pages. DuckDB did just as well on paper. The hive path cut it to 1 of 23 files, and the t-stats skipped the row group covering t 1672 to 2827, so it read 17 rows. It still took 8.89 ms, because every DuckDB query here pays a floor of approximately 6–9 ms (the fastest was 6.37 ms) to open files, read Parquet footers, decode a compressed column chunk and plan. Selective queries never get below that floor.

The as-of questions show the same thing at different sizes. Q1 is 104 lookups, so 104 index seeks in SQLite: 0.18 ms against 20.13 ms. DuckDB first builds the end-of-tick book over all 91,471 snapshots with a window function, then runs one ASOF join. For my 38 fills that bulk work is wasted (0.20 ms against 19.06 ms backward, 0.24 ms against 23.05 ms forward). For all 9,017 fills it is the right tool. SQLite pays one or two index seeks per fill, about 3.9 µs per probe (34.93 ms). DuckDB's 21.73 ms is no slower than its 38-fill run, because its cost follows the size of the tables, not the number of probes. At these rates the two meet at roughly 5,600 probes on two weeks of data, and each added week pushes that point lower.

SQLite's win also depends on the planner using the index. For the forward markout's "last snapshot inside tick T", SQLite chose the (week, seq) primary key and scanned the whole week once per fill. In an earlier run on Week 2 alone (not part of the table above) that took 6,349 ms. Forcing INDEXED BY book_wst brought it to 4.3 ms. DuckDB's ASOF join does not have this.

The full scan turns this around. SQLite stores rows, so to average 4 columns it reads all 2,461 pages (10.1 MB) of book_snapshot, every column of every row: 59.43 ms. Parquet stores columns, so DuckDB reads only the 4 needed column chunks, 199 KB compressed of the table's 2.99 MB (6.7 %), and aggregates a vector at a time: 8.71 ms. Row-group statistics don't help a query that filters nothing. The same layout makes the Parquet store 9.9× smaller than SQLite (8.7 vs 86.4 MB). Typed columns compress well under zstd, and depth stays as lists, where SQLite spends 42.0 MB on 1,516,236 normalised book_level rows, each with its own key.


## 3. Backtest

The engine is the lab's `backtest_parquet.run`, unchanged, reading Parquet. It runs on the logical clock only: at tick t the strategy sees books and events with tick ≤ t. Every signal is an IOC due at (t + 1) that sweeps the recorded depth as a taker and pays the venue's 0.30 % taker fee** (every one of the 9,017 fills on both tapes has `fee / notional` = 0.003). It is long-only with a position cap of 400 and 20 granted shares per name. That is Week 2's rule; I kept it for Week 4, which allows shorts, so the comparison holds still. `ts` is read only to pace the replay. The IS/OOS split t = 650 was fixed before any run, halfway between Week 2's scheduled events (t400 and t900). RampTrail's parameters were also fixed before any
run, and the Week 4 tape arrived after the strategy was frozen, so all of Week 4 is out-of-sample.

**My strategy**  The strategy trades the CALENDAR_EVENT message using only the message's own fields (pct, overshoot ramp_ticks). On good news it buys 200 of each affected name during the ramp, paying at most pre-mid × (1 + pct × overshoot), then exits with a 1.5 % trailing stop or after 120 ticks. On bad news (long-only) it sells what it holds during the ramp, no lower than that same bound, and buys it back once the ramp ends. A fee gate skips any event where |pct| × overshoot doesn't beat two taker fees.

| week | strategy | P&L $ | IS t<650 $ | OOS t≥650 $ | max DD $ | Sharpe/tick | fills | fees $ | fill digest |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---|
| 2 | mine | +1,152 | +245 | +907 | 1,253 | +0.0162 | 61 | 484 | `518c8ca1d045` |
| 2 | do_nothing | +198 | +220 | −22 | 1,931 | +0.0030 | 0 | 0 | `4f53cda18c2b` |
| 2 | buy_hold | +1,203 | +173 | +1,031 | 3,308 | +0.0067 | 56 | 170 | `b70459ca4a6a` |
| 2 | momentum (20 ticks, ±0.3 %) | −16,501 | −1,253 | −15,248 | 16,858 | −0.0547 | 1,187 | 9,931 | `17238e463468` |
| 4 | mine | −1,158 | −79 | −1,079 | 1,602 | −0.0205 | 53 | 368 | `e0339dfbc9e5` |
| 4 | do_nothing | −1,152 | −177 | −975 | 1,596 | −0.0218 | 0 | 0 | `4f53cda18c2b` |
| 4 | buy_hold | −1,591 | +22 | −1,613 | 6,459 | −0.0091 | 73 | 472 | `3964e9da732f` |
| 4 | momentum | +1,040 | +2,050 | −1,009 | 5,575 | +0.0106 | 464 | 3,027 | `77e99e576484` |

Sharpe = mean ÷ sample standard deviation (ddof = 1) of the per-tick equity changes (2,826 in Week 2, 2,397 in Week 4). It isn't annualised. "Fills" counts one record per price level swept.

**What this says.**

- Every P&L includes the 20 granted shares per name, marked to the mid, so do nothing is the real zero. Against it, my strategy added +954 in Week 2 and −6 in Week 4. Buy-and-hold added +1,005 and −439. Momentum added −16,699 and +2,192. My strategy had the smallest drawdown of any strategy that trades, in both weeks. In week 2, liquidity limited it. AAPL's ask side held 36 shares at t401 and none from t402 to t414, so it bought 36 of 200. When the stop fired at t417 it sold them at 199.12 and 198.97, with the mid at 203.71. In week 4 (out of sample), all three events were weren't positive, and the move came in the first tick. The strategy's next-tick order sold AMZN's 20 at 170.15/169.86 at t351, already −4 % fromthe 177.16 pre-mid. Its GOOGL buy-back at t816 met a momentary 172.29 ask (19 of 20 shares filled at 172.29–172.54, after selling at 167.54), the same wide ask that hurts the bot'slogical-clock replay in §4. Net of fees (368), the bad-news leg came out flat.There is one event per side of the split per week, but it is still very little evidence.

**Determinism**

| week | no pacing | 50× real time | no pacing again |
|---:|---|---|---|
| 2 | +1,152.15 · 61 · `518c8ca1d045` | +1,152.15 · 61 · `518c8ca1d045` (60.0 s) | — |
| 4 | −1,158.11 · 53 · `e0339dfbc9e5` | −1,158.11 · 53 · `e0339dfbc9e5` (59.7 s) | −1,158.11 · 53 · `e0339dfbc9e5` |

**Control** The lab's EventRide on Week 2, t 390..450. The only
difference is the one-line `--realclock` bug, which times the exit with `time.time()`:

| | 2× | 10× |
|---|---|---|
| EventRide (logical clock) | +497.04 · 4 fills · `e4009208c4dc` | +497.04 · 4 fills · `e4009208c4dc` |
| EventRide `--realclock` | +814.86 · 5 fills · `0ff2a5acdc8a` | +892.66 · 1 fill · `3f316e92afdc` |

**Sensitivity**

| | Week 2 P&L | fills | Week 4 P&L | fills |
|---|---:|---:|---:|---:|
| latency 0 (fills at the decision tick: look-ahead, for scale only) | +1,164 | 66 | −1,482 | 54 |
| **latency 1, sweep the depth (the headline)** | **+1,152** | 61 | **−1,158** | 53 |
| latency 2 | +924 | 54 | −1,206 | 44 |
| latency 1, touch only | +1,148 | 65 | −1,360 | 58 |
| latency 1, sweep, no fee | +1,636 | 61 | −790 | 53 |

Fees were 484 and 368. The no-fee runs have the same digest as the headline runs, which shows the fee changes P&L but not a single fill.

---

## 4. Logical vs real clock: our bot on Friday Oct 23

*Sources: the Week 4 market tape, `stt_11_trader_1` (the shock + starter + IPO seat; `stt_11_trader_2`, the mean-reversion seat, has no fills on it), and `team/trader.py` run unchanged in `replay_bot.py`.*

A tick is not a second. The block was 2,985.1 s for 2,397 ticks = 1.245 s per tick (median 1.242 s, p10 0.755 s, p90 1.685 s, p99 2.534 s, max 8.971 s). 19.5 % of ticks were longer than 1.5 s. Week 2 averaged 1.062 s. The bot hard-codes `EXCHANGE_TICK_SEC = 1.0` and converts every timer from seconds, so on Oct 23 each of them ran about 20 % short in ticks: the 15-"tick" shock ramp was about 12 ticks, the 10 s starter hold was 8 ticks (live: bought at t0, sold at t8), the 120 s event pause was about 96 ticks, and the 60 s mean-reversion max hold was about 48 ticks.

Live vs replay of the same `on_tick` (trading P&L = fill cash flows − fees + net shares at
the last mid, computed the same way for both):
| stt_11_trader_1, Oct 23 | fills | GOOGL sold | GOOGL bought | AMZN | MSFT (starter) | fees | P&L |
|---|---:|---|---|---|---|---:|---:|
| live | 22 | 66 @ 173.59 (t800) | 50 @ 164.15 (t812–813) | no trade | 4 round trips, −56.34 | 114.76 | +369.96 |
| replay, venue clock, same-tick fills | 31 | 90 @ 173.66 (t800) | 70 @ 164.40 (t812) | short 170 @ 176.96 / cover 150 @ 164.37 = +1,997 | 1 round trip | 258.18 | +2,569.13 |
| replay, venue clock, next-tick fills (backtester rule) | 4 | 20 @ 162.43 (t803) | — | order unfilled | 1 round trip | 22.30 | −229.41 |
| replay, logical clock (1 tick = 1 s), same-tick fills | 31 | 90 @ 173.66 | 70 @ 171.43 (t815) | +2,002 | 1 round trip | 259.64 | +2,080.31 |

Venue clock means `time.monotonic()` inside the bot returns the tape's `ts`.

* Speed inside the tick is the whole trade. Live, the GOOGL short filled 0.07 s after the event line, in the same tick (+0 ticks), at 173.82 down to 173.35. The replay with same-tick fills hits the same levels (173.90 down to 173.35). It got 90 shares to live's 66 because someone else was faster at the top level live. With the backtester's next-tick rule, the 420-share orders priced off the pre-event book (AMZN @ 176.25, GOOGL @ 173.26) arrive after the bids are gone and don't fill, and P&L flips to −229. A logical-clock backtest can't represent this edge. It can only bound it with latency 0 against latency 1.
* The cover was timed in seconds. The bot covered GOOGL at t812, 12 ticks into a 15-tick ramp, because `elapsed = seconds / 1.0` reached 15 after 15.6 s. The logical-clock replay waits the real 15 ticks and covers at t815 into a momentary 172.29 ask: GOOGL P&L +91 instead of +585. Which clock the timers read moved this replay by $489.
* The live bot didn't trade AMZN. The replay says that at t350 (15:47:41 CDT) it would have shorted 170 into the drop (+$1,997). The market tape can't say why. The MSFT starter round trips at t1232, t2272 and t2348 point to the bot restarting or reconnecting during the block. Check the logs around 15:47:41 CDT.
* The bot isn't deterministic under replay. Two runs on the venue clock give the same digest (`8a312f1786cc`). The same code on the real `time.monotonic()` at 10× gives `4e5419840860` (+1,750.65, 36 fills), and at 20× gives `19b7dfceab4d` (+1,74256, 34 fills).

Every clock read in `team/trader.py` (17 calls, all `time.monotonic()`, plus the `EXCHANGE_TICK_SEC` constant):

| line, function | what it times | decision path or edge |
|---|---|---|
| 366 `_trade_shock`, 815 `_start_shock` | shock `elapsed` in "ticks" = seconds ÷ 1.0: entry window, cover/exit at `ramp`, fade window `ramp ± 2` | decision. This is the t812 cover. Should count venue ticks. |
| 209 `on_tick` | MR max hold 60 s, event pauses; also the 5 s reconcile and 60 s status log | decision (MR hold, pauses) + edge (reconcile, logging) |
| 332 `_trade_starter`, 707 `on_fill` | starter hold 10 s (= 8 ticks live), order gap | decision |
| 604 `_resumed`, 739/804 `on_event`, 810 `_start_shock` | 120 s symbol pause after an event / resume | decision|
| 588 `_is_halted`, 660 `on_reject` | halt length (the venue gives it in seconds) | edge: a venue rule written in wall time, so wall time is right here |
| 667/670 `_is_pending`, 674 `_shock_order`, 688 `on_fill` | 1.0 s pending timeout, 0.4 s order gap, 0.15 s fill settle | edge: waiting for the network, but it gates decisions, so it changes fills under replay |
| 579 `_in_flight`, 584 `_note_sent` | 2.0 s in-flight shares in the risk gate | edge (network), feeds sizing |
| logging `asctime` | log timestamps | edge |

Week 2 for comparison: the older bot (trader_2 was then the shock seat) acknowledged its first AAPL order at t400, +0 ticks / +0.07 s after the earnings line.

The fix is to time every decision path in venue ticks, taken from the book or event messages, and keep `time.monotonic()` only for network edges and the venue's own wall-time rules (halt length). Then a replay of the bot gives the same fill digest at any speed.

What the replay doesn't model: IPO allocation, dividends, the borrow cap, other seats on the team limit, the live `on_tick` cadence (the bot's own timer, not one call per venue tick), and positions carried into the week (it starts from the 20 granted shares).
