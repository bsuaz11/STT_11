from __future__ import annotations
import math
import time
from collections import deque
import numpy as np
import pandas as pd

FAST, SLOW, VOL = 20, 100, 100
CLIP = 3.0
LOOKBACK = max(FAST, SLOW, VOL + 1) - 1

def signal_vectorized(df: pd.DataFrame) -> np.ndarray:
    close = pd.Series(df["close"].to_numpy(dtype = "float64"))
    sym = pd.Series(df["symbol"].array)
    g = close.groupby(sym, observed = True)
    fast = g.transform(lambda s: s.rolling(FAST, min_periods = FAST).mean())
    slow = g.transform(lambda s: s.rolling(SLOW, min_periods = SLOW).mean())
    vol = g.transform(lambda s: s.pct_change().rolling(VOL).std())
    sig = ((fast - slow) / (vol * close)).to_numpy(dtype="float32")
    return np.clip(sig, -CLIP, CLIP)

def signal_naive(df: pd.DataFrame) -> np.ndarray:
    """The baseline: one Python loop over the rows, recomputing every window from scratch."""
    out = np.full(len(df), np.nan, dtype = "float32")
    hist: dict[str, deque] = {}
    for i, row in enumerate(df[["symbol", "close"]].itertuples(index = False)):
        h = hist.get(row.symbol)
        if h is None:
            h = hist[row.symbol] = deque(maxlen = VOL + 1)
        h.append(float(row.close))
        if len(h) < VOL + 1:
            continue
        c = list(h)
        fast = sum(c[-FAST:]) / FAST
        slow = sum(c[-SLOW:]) / SLOW
        rets = [c[k] / c[k - 1] - 1.0 for k in range(1, VOL + 1)]
        m = sum(rets) / VOL
        vol = math.sqrt(sum((r - m) ** 2 for r in rets) / (VOL - 1))
        s = (fast - slow) / (vol * c[-1])
        out[i] = min(max(s, -CLIP), CLIP)
    return out

def compute_partition(part: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    return part.index.to_numpy(), signal_vectorized(part)

def sleep_task(seconds: float) -> float:
    time.sleep(seconds)
    return seconds
