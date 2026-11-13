## 3. Complexity

For reference, here are some variables and their definitions: **L** = live price levels on the relevant side; **N** = resting orders; **k** = levels emptied by one incoming order; **m** = maker orders it fully consumes (m ≥ k); **d** = dead (cancelled) entries it skips; **S** = stale heap entries. Hashing is assumed O(1) expected, and "amortised" uses the accounting argument given below the table.

| Operation | Cost | Why |
|---|---|---|
| `add`, no match, **existing** level | **O(1)** | Index insert, `deque.append`,counter updates, and then also one `_best_level` peek at the opposite top is all O(1) when that top is live |
| `add`, no match, **new** level | **O(log L)** worst, O(1) expected for random prices | One `heappush` means a new best level sifts to the root (log₂L swaps) & a random price sifts O(1) levels on average |
| `add`, partial fill (stays inside the best level and then rests or not) | **O(m + 1)**, add O(log L) if the remainder opens a new level | Each consumed maker is O(1) (`popleft` and dict delete). Then the partially filled maker is updated in place |
| `add`, full sweep of k levels | **O(m + k log L)** amortised (+ O(d) dead skips, already paid for by their cancels) | Each emptied level costs a dict delete now and one `heappop` (O(log L)) when its stale entry reaches the top. This is the next `_best_level` call |
| `cancel` | **O(1)** amortized, O(1) expected per call apart from rebuilds | Dict pop + counter updates. The heap and deque are not touched and compaction is paid for by the whatever triggered it |
| `best_bid` / `best_ask` | **O(1)** when the top is live; **O(s log L)** when s amount of stale tops must be popped where  **amortized O(1)** per call | Each heap entry is pushed once and popped at most once so the pops are charged to the `cancel`/sweep that made the entry stale (O(log L) each) |
| `depth(side, n)` | O(L log n) | `heapq.nsmallest` over the live-level dict. A heap has no sorted traversal |

**Amortization argument** Every heap entry is created by exactly one `heappush` which made when a level is first created and destroyed by at most one `heappop` or one rebuild. If each level creation 2·log L credits then one pays for its push and the other pays for its eventual pop. Heap rebuilds cost O(heap size) and can happen only when stale entries exceed L + 64 so each stale entry pays O(1) towards them. Deque compactions cost O(live + dead) and they happen only when dead > live and are also paid for by the dead entries. Over any sequence of operations, total heap work is ≤ (levels created) · O(log L), and everything else is O(1) per order or per fill.

**Worst cases** A single `best_bid` call after S levels at the top were cancelled costs O(S log L) which is not O(1) but it is paid for. A sweep is linear in the number of fills it produces (m) which we can't avoid because the output has m elements.

## 4. Benchmark

**Setup.** Random inputs are generated before timing. Throughput uses one `perf_counter` pair around the whole loop with no per-op timers. Latencies use a `perf_counter_ns` pair per call minus the median timer overhead of 80 ns. Each configuration ran 3 times and the run with the best throughput is reported. GC is on unless stated otherwise and prices are integer ticks around 10,000.

Workloads:
* **Passive.** Non-crossing limits with geometrically distributed distance from the touch. About 190–330 levels in total, so N grows while L stays roughly fixed.
* **Mixed.** 55% limits at prices ~N(mid ± 3, 8 ticks), about 30% of which cross. 35% cancels of a random previously submitted limit (some of these were already filled, so the call returns `False`). 10% market orders. `best_bid()` is timed after every op.

### 4.1 Throughput and latency vs N (µs; p50 / p99 / p99.9 / max)

**Passive inserts (add, no match)**

| N | throughput (orders/s) | mean | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|
| 10k | 576k | 1.52 | 1.35 | 3.47 | 36.2 | 197 |
| 100k | 1,003k | 1.05 | 0.79 | 1.90 | 21.5 | 1,942 |
| 1M | 650k | 1.65 | 0.82 | 3.26 | 24.5 | **152,095** |

`best_bid` on the final book (100k calls): mean 0.14–0.32, p99 0.22–0.40 µs at every N.

**Mixed flow, N = 1M ops (863,923 ops/s overall)**

| op | count | mean | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|
| add, rests without trading | 383,142 | 1.50 | 1.16 | 3.66 | 20.5 | 7,572 |
| add, partial (1 level) | 109,247 | 1.56 | 1.07 | 5.15 | 21.8 | 352 |
| add, sweep (> 1 level, avg 2.22) | 56,692 | 2.79 | 2.34 | 7.85 | 27.6 | 97 |
| market | 99,996 | 1.46 | 1.08 | 5.00 | 20.3 | 946 |
| cancel | 350,923 | 0.60 | 0.41 | 1.60 | 12.6 | 2,082 |
| best_bid | 1,000,000 | 0.22 | 0.18 | 0.53 | 1.5 | 322 |

At 10k and 100k ops the mixed throughput was 1.10M and 1.05M ops/s. p50 values: add-rest 1.05 / 1.05, cancel 0.14 / 0.20, best_bid 0.21 / 0.17 µs (full data in `results.json`).

**GC effect, 1M passive inserts**

| | mean | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|
| gc on | 1.41 | 0.80 | 2.80 | 22.0 | 136,827 |
| gc off | 0.88 | 0.79 | 2.43 | 13.4 | 7,391 |

### 4.2 Scaling experiments

* **(a) Latency vs N** in the mixed flow p50.
* **(b) Heap stress vs L.** Each insert creates a **new best** ask (strictly falling prices), so `heappush` always sifts to the root. That is the log L worst case. The orange line is a control with the same L but prices in random order. The green line drains the book: each step cancels the top level and calls `best_ask()`, which pops one stale entry. Latencies are organized by the current L, up to L = 2²⁰ ≈ 1M levels.
* **(c) Sweep vs k.** A market order empties exactly k of 2,048 one-lot levels. The book is rebuilt (untimed) before each of 300 trials per k.

Isolated heap term (`heap_micro.py`: `heappush` of a new minimum + `heappop`, at fixed size L, best of 5 × 200k):

| L | 2⁶ | 2⁸ | 2¹⁰ | 2¹² | 2¹⁴ | 2¹⁶ | 2¹⁸ | 2²⁰ |
|---|---|---|---|---|---|---|---|---|
| ns / (push + pop) | 203 | 246 | 257 | 293 | 334 | 358 | 385 | 419 |

### 4.3 Does it match the predicted asymptotics?

**Independence from N (O(1) add/cancel/best_bid): yes at p50, with two explained deviations.**
p50 for `add` (≈ 0.8–1.2 µs) and `best_bid` (≈ 0.12–0.2 µs) stays flat across two orders of magnitude of N (panel a). The deviations:

1. **Passive mean and max grow at 1M because of the garbage collector but not the algo.** With GC off the mean drops from 1.41 to 0.88 µs and the max from 137 ms to 7 ms, while p50 is unchanged. Each resting order is a `_Order` object that holds references, so CPython tracks it for GC. With 1M of them, the occasional older-generation collection traverses the whole book, which gives the 100+ ms outliers. Still costs O(1) per operation, but the tail is not. A production engine would pre-allocate, `gc.freeze()` the book, or store orders in flat arrays. (allocation, const factor effect)
2. **Cancel p50 rises from 0.14 to 0.41 µs between 10k and 1M ops.** The resting book grows from about 770 to 69k orders, so a cancel looks up a random key in a larger dict and touches a random, cache-cold `_Order` and `_Level`. The operation count is the same, but the memory latency per operation is higher. The share of cancels that hit already-filled orders (cheaper, return `False`) also changes with N. There is no algorithmic N term. (cache miss effect)

The 10k passive run is *slower* than 100k. That is warm-up: CPython 3.13's adaptive interpreter specialises bytecode over the first few thousand calls, and dict/heap growth is amortised over fewer ops.

**O(log L) for new levels: present, but invisible at the book level, and this is expected.**
The isolated heap measurement is logarithmic. Push + pop increases by about 15 ns per doubling of L (203 to 419 ns from 2⁶ to 2²⁰), so the log term is about 7.5 ns × log₂L per heap operation, so about 150 ns at L = 1M. A full `add` costs about 1.3–2 µs and almost all of it a constant (interpreter dispatch, argument checks, allocating `_Order`/`_Level`/`deque` objects, and dict inserts). The log term is under 10% of the total so smaller than the ±20% bucket-to-bucket noise. Linear fits of p50 against log₂L give slopes of 0.2–30 ns with R² ≤ 0.4. So statistically, there is no measurable log signal at the `add` level. The rise and drop between L = 2¹⁴ and 2¹⁸ comes from single-pass buckets on a shared VM. Additionally, plus `dict` resizes that happen at powers of two. At about 1M levels, memory becomes more costly. Each empty `deque` is about 760 bytes so 1M levels is close to 1 GB. That is a constant-factor and cache argument for a tick-indexed array once the price range is bounded.

The random-price control (orange) is flat. That matches the known result that `heappush` of a random key does O(1) expected sifts. The O(log L) is a worst case, and it happens exactly when every new order improves the touch.

**Sweep cost O(m + k log L): yes.**
From my plot, p50 fits **6.6 + 0.97·k µs (R² = 0.9994)**: linear in k, at about 1 µs per level emptied. That 1 µs covers one fill tuple, one `popleft`, two dict deletes, one `heappop` at L ≈ 2,048 (about 11 sifts) and one `_best_level` call. The 4.5 µs intercept at k = 1 is higher than a warm partial fill in the mixed run (about 1.1 µs) because each trial is a single cold call made right after the book was rebuilt. Instruction and data caches and the branch predictor are all cold for the matching path. In the warm mixed flow, sweeps averaging 2.2 levels have a p50 of 2.3 µs, which agrees with the ~1 µs-per-level slope.

**Amortized O(1) `best_bid` with lazy deletion: yes.** In the drain experiment, every call has to pop one stale entry. It costs 0.4–0.56 µs from L = 64 to 2²⁰, which is the expected log-factor heap pop on top of a constant. When there is nothing stale, `best_bid` is a dict lookup at about 0.12–0.2 µs.