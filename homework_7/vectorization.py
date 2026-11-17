from __future__ import annotations
import argparse
import json
import multiprocessing as mp
import os
import pickle
import resource
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import numpy as np
import pandas as pd
from worker_func import LOOKBACK, compute_partition, signal_naive, signal_vectorized, sleep_task
HERE = os.path.dirname(os.path.abspath(__file__))

def peak_rss_mb() -> float:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) / 1e3
    except OSError:
        pass
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / 1e6 if sys.platform == "darwin" else r / 1e3

def build_untuned(n_symbols: int, n_bars: int, seed: int = 33500) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    base = rng.uniform(20, 500, size = n_symbols)
    rets = rng.normal(0, 0.001, size = (n_bars, n_symbols))
    close = np.round(base * np.exp(np.cumsum(rets, axis = 0)), 2)
    open_ = np.vstack([base.round(2)[None, :], close[:-1]])
    wick = np.abs(rng.normal(0, 0.0005, size=(2, n_bars, n_symbols)))
    high = np.round(np.maximum(open_, close) * (1 + wick[0]), 2)
    low = np.round(np.minimum(open_, close) * (1 - wick[1]), 2)
    volume = rng.poisson(5_000, size = (n_bars, n_symbols))
    stamps = pd.date_range("2026-01-05 09:30", periods = n_bars, freq="min").strftime("%Y-%m-%d %H:%M:%S").to_numpy()
    names = np.array([f"SYM{i:04d}" for i in range(n_symbols)])
    return pd.DataFrame({
        "timestamp": np.repeat(stamps, n_symbols),
        "symbol": np.tile(names, n_bars),
        "open": open_.ravel(), "high": high.ravel(), "low": low.ravel(), "close": close.ravel(),
        "volume": volume.ravel().astype("int64"),
    })


def tune(untuned: pd.DataFrame) -> pd.DataFrame:
    df = untuned.astype({"symbol": "category", "open": "float32", "high": "float32",
                      "low": "float32", "close": "float32", "volume": "int32"})
    df.index = pd.DatetimeIndex(pd.to_datetime(untuned["timestamp"], format = "%Y-%m-%d %H:%M:%S"), name = "timestamp")
    return df.drop(columns = "timestamp")


def mem_table(df: pd.DataFrame) -> dict[str, tuple[str, float]]:
    per = df.memory_usage(deep=True, index=True) / 1e6
    out = {"(index)": (str(df.index.dtype), float(per["Index"]))}
    out.update({c: (str(df[c].dtype), float(per[c])) for c in df.columns})
    out["TOTAL"] = ("", float(per.sum()))
    return out

def run_inmemory(csv: str, out_path: str) -> None:
    df = pd.read_csv(csv, usecols = ["symbol", "close"], dtype = {"symbol": "str", "close": "float32"})
    np.save(out_path, signal_vectorized(df))


def run_chunks(csv: str, out_path: str, n_rows: int, chunksize: int) -> None:
    out = np.lib.format.open_memmap(out_path, mode = "w+", dtype = "float32", shape = (n_rows,))
    carry = pd.DataFrame({"symbol": pd.Series([], dtype="str"), "close": pd.Series([], dtype = "float32")})
    pos = 0
    for chunk in pd.read_csv(csv, usecols = ["symbol", "close"], dtype = {"symbol": "str", "close": "float32"},
                             chunksize = chunksize):
        work = pd.concat([carry, chunk], ignore_index = True)   
        sig = signal_vectorized(work)
        out[pos:pos + len(chunk)] = sig[len(carry):]
        pos += len(chunk)
        carry = work.groupby("symbol", sort=False).tail(LOOKBACK)  
    assert pos == n_rows, (pos, n_rows)
    out.flush()

def mem_run(mode: str, csv: str, out_path: str, n_rows: int, chunksize: int) -> None:
    base = peak_rss_mb()
    t0 = time.perf_counter()
    if mode == "inmemory":
        run_inmemory(csv, out_path)
    else:
        run_chunks(csv, out_path, n_rows, chunksize)
    print(json.dumps({"mode": mode, "seconds": time.perf_counter() - t0,
                      "peak_rss_mb": peak_rss_mb(), "baseline_rss_mb": base}))

def spawn_mem_run(mode: str, csv: str, out_path: str, n_rows: int, chunksize: int) -> dict:
    cmd = [sys.executable, os.path.join(HERE, "vectorization.py"), "--mem-run", mode,
           "--csv", csv, "--out", out_path, "--n-rows", str(n_rows), "--chunksize", str(chunksize)]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True, cwd=HERE)
    return json.loads(res.stdout.strip().splitlines()[-1])

def partitions(frame: pd.DataFrame, n_parts: int) -> list[pd.DataFrame]:
    codes = frame["symbol"].cat.codes.to_numpy() % n_parts
    return [frame.iloc[np.flatnonzero(codes == k)] for k in range(n_parts)]

def run_pool(kind: str, workers: int, parts: list[pd.DataFrame], n_rows: int, ctx=None) -> tuple[float, float, np.ndarray]:
    nap = 0.2
    t0 = time.perf_counter()
    ex = ProcessPoolExecutor(max_workers = workers, mp_context=ctx) if kind == "process" else ThreadPoolExecutor(max_workers = workers)
    list(ex.map(sleep_task, [nap] * workers)) 
    startup = time.perf_counter() - t0 - nap
    t1 = time.perf_counter()
    results = list(ex.map(compute_partition, parts))
    work = time.perf_counter() - t1
    ex.shutdown()
    out = np.empty(n_rows, dtype = "float32")
    for idx, sig in results:
        out[idx] = sig
    return startup, work, out

def same(a: np.ndarray, b: np.ndarray) -> bool:
    return bool(np.array_equal(a, b, equal_nan = True))

def main(quick: bool) -> None:
    n_symbols, n_bars = (500, 10_000) if not quick else (200, 5_000)
    R: dict = {"python": sys.version.split()[0], "pandas": pd.__version__, "numpy": np.__version__,
               "cpus": os.cpu_count(), "start_method": mp.get_start_method(), "platform": sys.platform}
    print(f"Python {R['python']}, pandas {R['pandas']}, numpy {R['numpy']}, {R['cpus']} CPUs, start method '{R['start_method']}'\n")
    t0 = time.perf_counter()
    lazy = build_untuned(n_symbols, n_bars)
    mem_before = mem_table(lazy)
    df = tune(lazy)
    del lazy
    mem_after = mem_table(df)
    N = len(df)
    R["rows"], R["symbols"], R["bars"] = N, n_symbols, n_bars
    R["mem_before"], R["mem_after"] = mem_before, mem_after
    print(f"[1] {N:,} rows ({n_symbols} symbols x {n_bars:,} one-minute bars) in {time.perf_counter() - t0:.1f}s")
    print(f"    memory {mem_before['TOTAL'][1]:.1f} MB -> {mem_after['TOTAL'][1]:.1f} MB "
          f"({mem_before['TOTAL'][1] / mem_after['TOTAL'][1]:.1f}x smaller)")

    keep = df["symbol"].cat.categories[:20]
    sub = df[df["symbol"].isin(keep)]
    t0 = time.perf_counter(); z_naive = signal_naive(sub); t_naive = time.perf_counter() - t0
    t_vec_sub, z_vec = float("inf"), None
    for _ in range(3):
        t0 = time.perf_counter(); z_vec = signal_vectorized(sub); t_vec_sub = min(t_vec_sub, time.perf_counter() - t0)
    assert np.array_equal(np.isnan(z_naive), np.isnan(z_vec)), "different warm-up NaNs"
    ok = ~np.isnan(z_naive)
    assert np.allclose(z_vec[ok], z_naive[ok], rtol=1e-6, atol=1e-6)
    R["naive"] = {"rows": len(sub), "valid_rows": int(ok.sum()), "nan_rows": int((~ok).sum()),
                  "naive_s": t_naive, "vectorized_s": t_vec_sub, "speedup": t_naive / t_vec_sub,
                  "max_abs_diff": float(np.max(np.abs(z_vec[ok] - z_naive[ok]))),
                  "naive_full_estimate_s": t_naive * N / len(sub)}
    print(f"[2] subsample {len(sub):,} rows: naive {t_naive:.2f}s, vectorized {t_vec_sub:.3f}s -> "
          f"{t_naive / t_vec_sub:,.0f}x; allclose on {ok.sum():,} rows, max |diff| {R['naive']['max_abs_diff']:.1e}, "
          f"NaN warm-up identical ({(~ok).sum():,} rows)")

    frame = pd.DataFrame({"symbol": df["symbol"].array, "close": df["close"].to_numpy()})   # RangeIndex = row positions
    t_full = float("inf")
    for _ in range(2):
        t0 = time.perf_counter(); ref = signal_vectorized(frame); t_full = min(t_full, time.perf_counter() - t0)
    R["vectorized_full_s"] = t_full
    print(f"    full {N:,} rows vectorized: {t_full:.2f}s (the naive loop would take about {R['naive']['naive_full_estimate_s'] / 60:.0f} min)")

    with tempfile.TemporaryDirectory() as d:
        csv = os.path.join(d, "ohlcv.csv")
        t0 = time.perf_counter()
        df.to_csv(csv, float_format="%.2f")
        R["csv_mb"] = os.path.getsize(csv) / 1e6
        print(f"[3] wrote {R['csv_mb']:.0f} MB CSV in {time.perf_counter() - t0:.0f}s")
        chunksize = 250_000
        inm = spawn_mem_run("inmemory", csv, os.path.join(d, "inmem.npy"), N, chunksize)
        chk = spawn_mem_run("chunked", csv, os.path.join(d, "chunked.npy"), N, chunksize)
        a, b = np.load(os.path.join(d, "inmem.npy")), np.load(os.path.join(d, "chunked.npy"))
        assert np.array_equal(np.isnan(a), np.isnan(b)), "chunk boundary broke the warm-up"
        ok = ~np.isnan(a)
        assert np.allclose(a[ok], b[ok], rtol=1e-6, atol=1e-6)
        assert np.allclose(a[ok], ref[ok], rtol=1e-6, atol=1e-6)  
        R["ooc"] = {"chunksize": chunksize, "carry_rows_per_symbol": LOOKBACK, "inmemory": inm, "chunked": chk,
                    "max_abs_diff": float(np.max(np.abs(a[ok] - b[ok]))),
                    "bitwise_equal_frac": float(np.mean(a[ok] == b[ok]))}
        for r in (inm, chk):
            print(f"    {r['mode']:9s} {r['seconds']:6.1f}s  peak RSS {r['peak_rss_mb']:7.0f} MB "
                  f"(interpreter + pandas alone: {r['baseline_rss_mb']:.0f} MB)")
        print(f"    chunked == in-memory: allclose, max |diff| {R['ooc']['max_abs_diff']:.1e}, "
              f"{R['ooc']['bitwise_equal_frac']:.1%} bitwise equal")

    n_parts = 32
    parts = partitions(frame, n_parts)
    t0 = time.perf_counter(); blobs = [pickle.dumps(p, protocol=pickle.HIGHEST_PROTOCOL) for p in parts]; t_pk = time.perf_counter() - t0
    t0 = time.perf_counter(); _ = [pickle.loads(b) for b in blobs]; t_upk = time.perf_counter() - t0
    res_blobs = [pickle.dumps(compute_partition(p), protocol=pickle.HIGHEST_PROTOCOL) for p in parts[:4]]
    R["pickle"] = {"partitions": n_parts, "mb": sum(map(len, blobs)) / 1e6, "dumps_s": t_pk, "loads_s": t_upk,
                   "result_mb_est": sum(map(len, res_blobs)) / 1e6 * n_parts / 4}
    del blobs, res_blobs
    print(f"[4] {n_parts} partitions by symbol: pickle {R['pickle']['mb']:.0f} MB in {t_pk * 1e3:.0f} ms, "
          f"unpickle {t_upk * 1e3:.0f} ms; results back ~{R['pickle']['result_mb_est']:.0f} MB")

    counts = sorted({1, 2, 4, os.cpu_count() or 1})
    R["pool"] = []
    for kind in ("process", "thread"):
        for w in counts:
            best = None
            for _ in range(2):
                st, wk, out = run_pool(kind, w, parts, N)
                assert same(out, ref), f"{kind} x{w} differs from the single-process output"
                if best is None or wk < best[1]:
                    best = (st, wk)
            R["pool"].append({"kind": kind, "workers": w, "startup_s": best[0], "work_s": best[1],
                              "speedup_vs_single": t_full / best[1]})
            print(f"    {kind:7s} x{w:<3d} start-up {best[0] * 1e3:7.1f} ms   work {best[1]:6.2f}s   "
                  f"{t_full / best[1]:4.2f}x vs one process   (== single-process: yes)")
    st, wk, out = run_pool("process", counts[-1], parts, N, ctx=mp.get_context("spawn"))
    assert same(out, ref)
    R["spawn"] = {"workers": counts[-1], "startup_s": st, "work_s": wk}
    print(f"    spawn   x{counts[-1]:<3d} start-up {st * 1e3:7.1f} ms   work {wk:6.2f}s   (macOS / Windows default)")

    with open(os.path.join(HERE, "results.json"), "w") as f:
        json.dump(R, f, indent=2)
    with open(os.path.join(HERE, "results.md"), "w") as f:
        f.write(markdown(R))
    print("\nwrote results.json and results.md")

def markdown(R: dict) -> str:
    L = [f"_Machine: {R['cpus']} CPUs, Python {R['python']}, pandas {R['pandas']}, numpy {R['numpy']}, "
         f"{R['platform']}, default start method `{R['start_method']}`._", "",
         f"### Memory: {R['rows']:,} rows ({R['symbols']} symbols x {R['bars']:,} bars)", "",
         "| column | before dtype | MB | after dtype | MB |", "|---|---|---:|---|---:|"]
    b, a = R["mem_before"], R["mem_after"]
    for c in a:
        if c == "TOTAL":
            continue
        bd, bm = b.get(c, b["timestamp"] if c == "(index)" else ("", 0.0))
        if c == "(index)":
            L.append(f"| index / timestamp | RangeIndex + `{b['timestamp'][0]}` column | {b['(index)'][1] + b['timestamp'][1]:.1f} | `{a[c][0]}` | {a[c][1]:.1f} |")
        else:
            L.append(f"| {c} | `{bd}` | {bm:.1f} | `{a[c][0]}` | {a[c][1]:.1f} |")
    L.append(f"| **total** | | **{b['TOTAL'][1]:.1f}** | | **{a['TOTAL'][1]:.1f}** ({b['TOTAL'][1] / a['TOTAL'][1]:.1f}x smaller) |")
    n = R["naive"]
    L += ["", "### Naive loop vs vectorized", "",
          "| | rows | seconds |", "|---|---:|---:|",
          f"| naive per-row loop (`itertuples`) | {n['rows']:,} | {n['naive_s']:.2f} |",
          f"| vectorized (best of 3) | {n['rows']:,} | {n['vectorized_s']:.3f} |",
          f"| **speedup** | | **{n['speedup']:,.0f}x** |",
          f"| vectorized, full dataset | {R['rows']:,} | {R['vectorized_full_s']:.2f} |",
          f"| naive, full dataset (extrapolated) | {R['rows']:,} | ~{n['naive_full_estimate_s']:,.0f} |", "",
          f"Equivalence: identical NaN warm-up on {n['nan_rows']:,} rows (first 100 bars of each symbol); "
          f"`np.allclose(rtol=1e-6, atol=1e-6)` on the other {n['valid_rows']:,} rows, max |diff| {n['max_abs_diff']:.1e}."]
    o = R["ooc"]
    L += ["", f"### Out of core ({R['csv_mb']:.0f} MB CSV, chunks of {o['chunksize']:,} rows, "
          f"{o['carry_rows_per_symbol']} rows carried per symbol)", "",
          "| run (fresh process each) | seconds | peak RSS MB | of which interpreter + pandas |", "|---|---:|---:|---:|"]
    for k in ("inmemory", "chunked"):
        r = o[k]
        L.append(f"| {k} | {r['seconds']:.1f} | {r['peak_rss_mb']:.0f} | {r['baseline_rss_mb']:.0f} |")
    L += ["", f"Equivalence: same NaN positions; `allclose(rtol=1e-6)` with max |diff| {o['max_abs_diff']:.1e} "
          f"({o['bitwise_equal_frac']:.1%} of values bitwise equal); both also match the frame built in memory."]
    p = R["pickle"]
    L += ["", f"### Processes vs threads ({p['partitions']} partitions by symbol; one process, no pool: "
          f"{R['vectorized_full_s']:.2f}s)", "",
          "| executor | workers | pool start-up ms | work s | speedup vs one process |", "|---|---:|---:|---:|---:|"]
    for r in R["pool"]:
        L.append(f"| {r['kind']} | {r['workers']} | {r['startup_s'] * 1e3:.1f} | {r['work_s']:.2f} | {r['speedup_vs_single']:.2f}x |")
    s = R["spawn"]
    L.append(f"| process (`spawn`, macOS/Windows default) | {s['workers']} | {s['startup_s'] * 1e3:.1f} | {s['work_s']:.2f} | "
             f"{R['vectorized_full_s'] / s['work_s']:.2f}x |")
    L += ["", f"Overhead, measured separately: pickling all {p['partitions']} partitions = {p['mb']:.0f} MB in "
          f"{p['dumps_s'] * 1e3:.0f} ms (unpickling {p['loads_s'] * 1e3:.0f} ms); results coming back ~{p['result_mb_est']:.0f} MB. "
          "Every parallel run returned exactly the single-process array (`np.array_equal`, NaNs included).", ""]
    return "\n".join(L)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--mem-run", choices=["inmemory", "chunked"])
    ap.add_argument("--csv"); ap.add_argument("--out")
    ap.add_argument("--n-rows", type=int); ap.add_argument("--chunksize", type=int, default=250_000)
    a = ap.parse_args()
    if a.mem_run:
        mem_run(a.mem_run, a.csv, a.out, a.n_rows, a.chunksize)
    else:
        main(a.quick)
