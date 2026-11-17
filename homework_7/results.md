_Machine: 8 CPUs, Python 3.11.7, pandas 2.2.3, numpy 1.26.4, darwin, default start method `spawn`._

### Memory: 5,000,000 rows (500 symbols x 10,000 bars)

| column | before dtype | MB | after dtype | MB |
|---|---|---:|---|---:|
| index / timestamp | RangeIndex + `object` column | 380.0 | `datetime64[ns]` | 40.0 |
| symbol | `object` | 320.0 | `category` | 10.0 |
| open | `float64` | 40.0 | `float32` | 20.0 |
| high | `float64` | 40.0 | `float32` | 20.0 |
| low | `float64` | 40.0 | `float32` | 20.0 |
| close | `float64` | 40.0 | `float32` | 20.0 |
| volume | `int64` | 40.0 | `int32` | 20.0 |
| **total** | | **900.0** | | **150.0** (6.0x smaller) |

### Naive loop vs vectorized

| | rows | seconds |
|---|---:|---:|
| naive per-row loop (`itertuples`) | 200,000 | 4.40 |
| vectorized (best of 3) | 200,000 | 0.073 |
| **speedup** | | **60x** |
| vectorized, full dataset | 5,000,000 | 2.60 |
| naive, full dataset (extrapolated) | 5,000,000 | ~110 |

Equivalence: identical NaN warm-up on 2,000 rows (first 100 bars of each symbol); `np.allclose(rtol=1e-6, atol=1e-6)` on the other 198,000 rows, max |diff| 0.0e+00.

### Out of core (302 MB CSV, chunks of 250,000 rows, 100 rows carried per symbol)

| run (fresh process each) | seconds | peak RSS MB | of which interpreter + pandas |
|---|---:|---:|---:|
| inmemory | 4.7 | 782 | 93 |
| chunked | 9.3 | 309 | 93 |

Equivalence: same NaN positions; `allclose(rtol=1e-6)` with max |diff| 0.0e+00 (100.0% of values bitwise equal); both also match the frame built in memory.

### Processes vs threads (32 partitions by symbol; one process, no pool: 2.60s)

| executor | workers | pool start-up ms | work s | speedup vs one process |
|---|---:|---:|---:|---:|
| process | 1 | 643.3 | 1.94 | 1.34x |
| process | 2 | 848.8 | 1.14 | 2.28x |
| process | 4 | 1785.1 | 1.30 | 2.00x |
| process | 8 | 5215.4 | 4.25 | 0.61x |
| thread | 1 | 19.7 | 6.33 | 0.41x |
| thread | 2 | 6.0 | 2.31 | 1.12x |
| thread | 4 | 12.4 | 1.78 | 1.46x |
| thread | 8 | 10.7 | 1.85 | 1.41x |
| process (`spawn`, macOS/Windows default) | 8 | 3710.1 | 3.01 | 0.86x |

Overhead, measured separately: pickling all 32 partitions = 70 MB in 20 ms (unpickling 28 ms); results coming back ~61 MB. Every parallel run returned exactly the single-process array (`np.array_equal`, NaNs included).
