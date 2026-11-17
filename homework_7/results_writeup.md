**Equivalence checks (all asserted in the code):**
- naive == vectorized: identical NaN warm-up, then `np.allclose(rtol=1e-6, atol=1e-6)`.
- chunked == in-memory: identical NaN positions, then `allclose`. They both also match the frame built in memory.
- parallel == single-process: `np.array_equal(..., equal_nan=True)` for every executor and worker count.

## Timing Tables
### Naive loop vs vectorized
| | rows | seconds |
|---|---:|---:|
| naive per-row loop (`itertuples`) | 200,000 | 4.40 |
| vectorized (best of 3) | 200,000 | 0.073 |
| **speedup** | | **60x** |
| vectorized, full dataset | 5,000,000 | 2.60 |
| naive, full dataset (extrapolated) | 5,000,000 | ~110 |

Equivalence: identical NaN warm-up on 2,000 rows (first 100 bars of each symbol) & `np.allclose(rtol=1e-6, atol=1e-6)` on the other 198,000 rows, max |diff| 0.0e+00.

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

Every parallel run returned exactly the single-process array (`np.array_equal`, NaNs included).

## Memory Numbers 
### Dtype Tuning
500 symbols * 10 Bars

| column | before dtype | MB | after dtype | MB |
|---|---|---:|---|---:|
| index / timestamp | RangeIndex + object (str) column | 380.0 | datetime64[ns] | 40.0 |
| symbol | object (str) | 320.0 | category | 10.0 |
| open | float64 | 40.0 | float32 | 20.0 |
| high | float64 | 40.0 | float32 | 20.0 |
| low | float64 | 40.0 | float32 | 20.0 |
| close | float64 | 40.0 | float32 | 20.0 |
| volume | int64 | 40.0 | int32 | 20.0 |
| **total** | | **900.0** | | **150.0** (6.0 times smaller) |

### Out of core (302 MB CSV, chunks of 250,000 rows, 100 rows carried per symbol)

| run (fresh process each) | seconds | peak RSS MB | of which interpreter + pandas |
|---|---:|---:|---:|
| inmemory | 4.7 | 782 | 93 |
| chunked | 9.3 | 309 | 93 |


## Four Explanation Paragraphs 

#### A. Where the vectorized version still copies data and how you would avoid it
There are three transform calls, each during which splits close into one series per symbol. Each symbol is interleaved which means that every split is a gather which is a copy. Then each lambda allocates the rolling calculation and transform() puts the pieces back into row by row order which is another full copy. Then to_numpy() and np.clop make another copy. An upcast to float64 is another copy as well. I would avoid all thi data copying by sorting once by (symbol, timestamp) so that each symbol is a consecutive slice and the grouping produces views instead of gathers. Then I would compute the three windows in one pass per group instead of doing three separate splits. Lastly, do arithmetic in place using np.subtract(), np.multiply(), etc., and then write into a pre-allocated float32 output.

#### B. Why a shift() is mandatory before you use any signal for trading (look-ahead bias)
A shift() is needed before using any signal for trading because if not used, then we're using future data to calculate signal[t]. So signal at time t is built from close at time t, and given that I only know what close at time t is when bar t has finished, it is not possible for me to have traded at that close or during the bar. signal[t] * return[t] only pays me the return of a signal that has already seen close[t] which results in the profit being inaccurate due to the added correlation of including a future time t. Runing df[signal].shift(1) would allow for me to calculate the return based on the previous row's signal. I am able to carry out this same logic on anything that is fit on the whole sample, for example z score.

#### C. When you would reach for numba, polars, or Dask instead — and why you are not reaching for it here
Numba, polars, and Dask are all libraries used to make data processing and computing more efficient than standard Python and pandas, however they are used in different situations. Numba compiles Python code to machine-ready code and one each row depends on the one prior so rolling windows can't be expressed. I would reach for Numba when im working with a path-dependent strategy where I don't need to calculate rolling values or cumulative ones. Polaris uses all CPU cores, with no GIL and optimizes queries by planning lazy queries before running the actual queries. I would use Polars when I'm calculating lots of rolling and grouped expressions on a larger set of data. Dask splits data into partitions and scheules them across across cores a cluster. This means that it is able to handle the case in which my data exceeds the machine's memory. So I would use Dask if my data didn't fit on my machine. I don't reach for any of numba, polars, or dask here because none of them pay off. After tuning dtypes, the frame is 150 MB, the chunked version only has 30 lines of pandas code, and the vectorized signal runs in 2.6s. Numba would only add a compile step and a dependency, making it less efficient. If I were to use polaris, it would result in rewriting code that's is different from the pandas API that the Arena uses. Lastly, if I were to use Dask, its scheduling per task overhead would be roughly equivalent to the 60 ms the each partition requires to complete. 

#### D. What your table shows about processes versus threads for this work (the GIL, and which numpy/pandas steps release it), and at what worker count adding processes stopped paying off and why.
My table shows that processes ver faster than threads, but only up to 2 workers. Two processes took 1.14 s of work, they were 2.3 times faster than one process with no pool (2.60 seconds). Four processes were slightly slower at 1.30 seconds and 8 processes much slower at 4.25 seconds. So adding processes stopped paying off after 2 workers. One of the reasons is that splitting the frame, pickling 70 MB of partition out, and unpickling and resembling 61 MB of results all stays serial in the parent so it doesn't shrink as additional workers are added. Another reason is that every worker is a separate Python with its own pandas which is 90 MB before any data. The last reason is that once you get tothe point where there are more workers than fast cores, they have to wait for the slower cores which causes additional workers to take more time as shown in the table. When using spawn, it launches a new interpreter for each worker which has to import pandas and numpy. Then you also have to pickle the inputs. These two steps together took 1.99 seconds for the two processes one. The steps that release it are pandas' compiled rolling kernels like roll_mean and roll_var and numpy's elementwise operations on large arrays: subtract, multiply, astype, clip, divide. The groupby split, the lambda call, and building each series, index and concat takes a large amoutn of time since there's 10K rows per group.