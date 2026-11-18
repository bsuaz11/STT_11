# Week 8 (Fri Nov 20): multi-venue, fee wars, hidden lead-lag links, the LUNA IPO

**Team STT_11:** Megan Maloney, Brett Suazo, Hannah Kong
**Run Friday:** `scripts/run_week8.sh` (trader_1 + broker_1 on every venue; logs in `Phase4/logs/`).
**Tests:** `tests/test_week8_bots.py` (25 tests). Full suite: 339 passed.

## What Friday checks, and how each is met
| Check | How | Evidence |
|---|---|---|
| Orders on both venues, routed on the NBBO and the fee difference | Every IOC passes `gate()` (risk), then `route()` (`team/venues.py`). Route picks the best price **after each venue's taker fee**, among venues that are live, fresh, not halted and within 1% of the primary's mark. A close goes to the venue holding the position. Opening orders skip venues charging more than 60 bps (the primary charged 200 bps in week 7, 400 in open play now). The probe (1 share while the primary is punitive) keeps the primary in use. | `test_route_*`, `test_trader_routes_an_opening_order_away_from_a_punitive_primary`, `test_the_probe_keeps_the_primary_in_use_at_one_share`; rehearsal on two local week-8 venues: `results/rehearsal/` |
| A bid in the LUNA bookbuild | `ipo` role: bids the top of the range (the sure allocation), $8k (~150 shares at $52), resent until confirmed. Kept small: the allocation can only be sold on the primary, at its fee. | `test_ipo_bid_for_luna` |
| Trade the hidden links: ≥10 follower orders inside the lag after a big leader move, ≥60% in the right direction | `leadlag` role (`team/leadlag.py`), below | Backtest on 300 recorded sessions: **≥15 such orders in every session (median 23), 100% in the predicted direction** (`results/leadlag_backtest.txt`) |
| Connected ≥80% | `run_week8.sh` restarts a dead seat in 3 s. Each extra venue reconnects on its own, and the router skips it while it's down. | |

## The three links (`leadlag_discovery.py`, `results/leadlag_discovery.txt`)
| Leader → follower | Direction | Lag (ticks, as seen in the feed) | Beta |
|---|---|---|---|
| AMD → GOOGL | same | 6 | +0.37 |
| NFLX → AMZN | opposite | 8 (9 on the earnings ramp) | −0.29 |
| MSFT → TSLA | opposite | 4 | −0.34 |

**Data:** 750 distinct sessions, 16.2M symbol-ticks. Parts 1, 3 and 4; part 2 wasn't downloaded, and `week08_history-2` is a second copy of part 4.

**Method.** Plain lagged correlation fails here: marks are smoothed (autocorrelation ~0.7) and share a market factor, so every pair correlates at lag 1. What works is an event study on big idiosyncratic leader ticks:
1. Take each stock's return minus the other candidates' average.
2. Find ticks where it's beyond 4σ.
3. Average the follower's idiosyncratic return at offsets 1–10.

Masking: scheduled prints (±80 ticks), halts and the open. Links are found greedily (strongest first, then both names leave the pool), and the echo tick is scored as the contrast between the echo and the revert that follows it.

**Confirmed three ways:**
- the first and second halves of every part give the same three links, lags and signs (t = 75–860);
- on NFLX's 750 earnings ramps, AMZN moves against NFLX starting 9 ticks later;
- isolated AMD steps (+15 bps) give GOOGL +5.5 bps at lag 6 and −5.5 the tick after.

**Pipeline: 13.5 s for 16.2M rows.** DuckDB reads only 3 Parquet columns, in parallel, straight into numpy. Each pass is vectorised over (sessions × ticks). No pandas, and no Python per row.

**How the engine plants a link** (`engine/exchange/server.py` advance_tick): the follower's fair value is multiplied by 1 + beta × the leader's one-tick return from `lag` ticks ago, for **one tick**. So the echo is a bump that mostly reverts. In quiet markets it's a few bps, under the spread. On a leader's earnings ramp it's 15–20 bps a tick for ~15 ticks. During the market-wide econ print there's no echo at all: calendar ramps set every stock's price directly.

## The lead-lag trade (`team/leadlag.py`)
- **Trigger:** the leader's one-tick mark return beyond 4σ (σ is an EWMA, learning from capped returns) and ≥ 4 bps. Returns are measured on each change of the venue mark, not on a clock.
- **Entry:** an IOC on the follower in direction sign(beta) × sign(leader move), sent the tick the leader moves, inside the lag.
- **Exit:** a ramp exits on the echo tick of its last trigger; each further ramp tick extends the hold. When NFLX's ramp retraces, AMZN keeps echoing the ramp for `lag` more ticks, so the exit waits for that. A marker exits one tick later, outside the window it's graded in.
- **Size:**
  - ramp (expected echo ≥ 8 bps): $20k;
  - marker (quiet market): $400, at most one per link per 90 ticks and 24 per session.
- **Stands aside** during market-wide prints, during the follower's own print, and while the follower is halted or not yet re-settled after a halt.
- **Backtest** (300 sessions, 10 bps fee a side, fills at the next tick's touch): **+$11 a session**. Ramps earn +$18k gross in total; markers cost about $0.50 each, which buys the graded orders.

## Broker (`team/broker.py`): what went wrong in week 7, and the fix
In the week 7 block the broker's net worth fell **$431k → $359k** while its realized P&L rose $12.6k. The cause was ~$390k of long inventory (≈150 shares of every stock) riding the econ print and two earnings moves, on a primary where makers paid 50 bps and takers 200 bps.

Week 8 changes:
- **Fee-aware width:** each venue's half-spread is at least its maker cost + 10 bps.
- **Punitive venues:** where makers pay more than 20 bps (the primary), the broker **stands aside**.
- **No IOC exits into big fees:** event flattens and stop exits only use an IOC where the taker fee is ≤ 50 bps. Elsewhere a stop pulls quotes; paying 2% to cross a coin-flip move is a sure loss.
- **Every venue:** `BROKER_VENUES=auto` quotes all student venues (positive rebates), including ours.

Tests: `test_broker_*`. In the rehearsal it stood aside on the 200 bps venue and quoted the 8 bps one.

## Exchange (`team/exchange.py`)
On Nov 17 our venue charged the highest taker fee of the student venues (14 bps, against 8, 8 and 11), so fee-aware routers skip it. The recommended schedule is **taker 7 / rebate 5**:
- it makes us the cheapest venue to take liquidity on;
- it still nets the 2 bps floor;
- our own broker's rebate stays inside the team.

**The hosted venue reads its fees from the portal (MY TEAM → MY VENUE), so this has to be set there; the code constant only documents it.**

## Before Friday
1. **Set the venue fees in the portal** (7 / 5), or keep 14 / 11 if you disagree.
2. **Practice run in open play** (it's live all week): `scripts/run_week8.sh` for ~10 minutes. In the logs, check:
   - `Venue … connected` for each student venue (the team token has only been tested on the primary);
   - `Venue fees`;
   - `Order: IOC … on <venue>` lines;
   - the broker's `STANDING ASIDE` on the primary.
   In open play the links aren't active, so marker trades cost a few dollars each (24 at most per session).
3. **Friday:** start by 15:35. The week switch is 15:30:20, the open is 15:40, LUNA's book is t150–t450.

## Limits
- Multi-venue was tested on two local engine venues with the course house bots, not yet on the hosted student venues.
- The primary's week-8 fee schedule is unknown; everything reads `FEE_SCHEDULE` live and routes accordingly.
- NFLX → AMZN shows lag 8 on quiet ticks and 9 on the earnings ramp. Entries go out at once, inside both.
- The marker trades are there for the check and cost money. The ramp trades (mainly NFLX's earnings at t700) carry the profit.
