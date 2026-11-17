from __future__ import annotations
import gc
import json
import platform
import random
import sys
import time
from orderbook import OrderBook

QUICK = "--quick" in sys.argv
SIZES = [10_000, 100_000] if QUICK else [10_000, 100_000, 1_000_000]
REPEATS = 1 if QUICK else 3
MID = 10_000
pc_ns = time.perf_counter_ns
def timer_overhead() -> int:
    s = []
    for _ in range(200_000):
        a = pc_ns(); b = pc_ns(); s.append(b - a)
    s.sort()
    return s[len(s) // 2]
OVH = timer_overhead()
 
def stats(ns: list[int]) -> dict:
    ns = sorted(max(0, x - OVH) for x in ns)
    n = len(ns)
    return {
        "count": n,
        "mean_ns": sum(ns) / n,
        "p50_ns": ns[n // 2],
        "p99_ns": ns[min(n - 1, int(n * 0.99))],
        "p999_ns": ns[min(n - 1, int(n * 0.999))],
        "max_ns": ns[-1],
    }
def gen_passive(n: int, seed: int) -> list[tuple]:
    """Non-crossing limit orders: bids below MID, asks at/above MID+1.
    Offsets are geometric-ish (more liquidity near touch); <= 200 levels/side."""
    rng = random.Random(seed)
    ops = []
    for i in range(n):
        off = min(200, int(rng.expovariate(1 / 15)) + 1)
        if rng.random() < 0.5:
            ops.append((i, "buy", float(MID - off), rng.randint(1, 100)))
        else:
            ops.append((i, "sell", float(MID + off), rng.randint(1, 100)))
    return ops

def gen_mixed(n: int, seed: int) -> list[tuple]:
    rng = random.Random(seed)
    ops, live = [], []
    nid = 0
    for _ in range(n):
        r = rng.random()
        if r < 0.35 and live:
            j = rng.randrange(len(live))
            live[j], live[-1] = live[-1], live[j]
            ops.append(("C", live.pop()))
        elif r < 0.45:
            nid += 1
            ops.append(("A", nid, rng.choice(("buy", "sell")), None, rng.randint(1, 60)))
        else:
            nid += 1
            side = rng.choice(("buy", "sell"))
            skew = -3 if side == "buy" else 3
            p = float(MID + round(rng.gauss(skew, 8)))
            ops.append(("A", nid, side, p, rng.randint(1, 100)))
            live.append(nid)
    return ops

def run_passive(n: int) -> dict:
    ops = gen_passive(n, seed = n)
    best = None
    for _ in range(REPEATS):
        b = OrderBook(); add = b.add
        t0 = time.perf_counter()
        for o in ops:
            add(*o)
        thr = n / (time.perf_counter() - t0)
        # latency run
        b = OrderBook(); add = b.add
        lat = [0] * n
        for i, o in enumerate(ops):
            t = pc_ns(); add(*o); lat[i] = pc_ns() - t
        bb = b.best_bid
        q = [0] * 100_000
        for i in range(100_000):
            t = pc_ns(); bb(); q[i] = pc_ns() - t
        res = {"throughput_ops_s": thr, "add": stats(lat), "best_bid": stats(q),
               "levels": len(b._levels["buy"]) + len(b._levels["sell"]), "resting": len(b)}
        if best is None or thr > best["throughput_ops_s"]:
            best = res
    return best


def run_mixed(n: int) -> dict:
    ops = gen_mixed(n, seed=n + 1)
    best = None
    for _ in range(REPEATS):
        b = OrderBook(); add, cancel = b.add, b.cancel
        t0 = time.perf_counter()
        for o in ops:
            if o[0] == "A":
                add(o[1], o[2], o[3], o[4])
            else:
                cancel(o[1])
        thr = n / (time.perf_counter() - t0)

        b = OrderBook(); add, cancel, bb = b.add, b.cancel, b.best_bid
        buckets = {"add_rest": [], "add_partial": [], "add_sweep": [], "market": [],
                   "cancel": [], "best_bid": []}
        swept = []
        for o in ops:
            if o[0] == "A":
                t = pc_ns(); fills = add(o[1], o[2], o[3], o[4]); dt = pc_ns() - t
                if o[3] is None:
                    buckets["market"].append(dt)
                elif not fills:
                    buckets["add_rest"].append(dt)
                else:
                    k = len({f[2] for f in fills})
                    swept.append(k)
                    (buckets["add_sweep"] if k > 1 else buckets["add_partial"]).append(dt)
            else:
                t = pc_ns(); cancel(o[1]); buckets["cancel"].append(pc_ns() - t)
            t = pc_ns(); bb(); buckets["best_bid"].append(pc_ns() - t)
        res = {"throughput_ops_s": thr,
               "ops": {k: stats(v) for k, v in buckets.items() if v},
               "mean_levels_swept_when_sweeping": (sum(k for k in swept if k > 1) /
                                                   max(1, sum(1 for k in swept if k > 1))),
               "final_levels": len(b._levels["buy"]) + len(b._levels["sell"]),
               "final_resting": len(b)}
        if best is None or thr > best["throughput_ops_s"]:
            best = res
    return best


def run_gc_effect(n: int) -> dict:
    ops = gen_passive(n, seed=n)
    out = {}
    for label, disable in (("gc_on", False), ("gc_off", True)):
        gc.collect()
        if disable:
            gc.disable()
        try:
            b = OrderBook(); add = b.add
            lat = [0] * n
            for i, o in enumerate(ops):
                t = pc_ns(); add(*o); lat[i] = pc_ns() - t
        finally:
            gc.enable()
        out[label] = stats(lat)
        del b, lat
    return out


def run_level_scaling(max_levels: int) -> dict:
    """Adversarial for the heap: every insert creates a NEW BEST ask level
    (strictly decreasing prices), so heappush sifts all the way to the root:
    log2(L) swaps.  Then drain: cancel the best level and call best_ask(),
    which pops one stale entry -> heappop = O(log L).
    Latencies are bucketed by current L (powers of two)."""
    b = OrderBook()
    add, cancel, ba = b.add, b.cancel, b.best_ask
    ins = {}
    for i in range(max_levels):
        price = float(10_000_000 - i)
        t = pc_ns(); add(i, "sell", price, 1); dt = pc_ns() - t
        ins.setdefault((i + 1).bit_length(), []).append(dt)
    # random-price control: same L, prices in random order (heap push is O(1) avg)
    rng = random.Random(7)
    prices = list(range(max_levels)); rng.shuffle(prices)
    b2 = OrderBook(); add2 = b2.add
    rnd = {}
    for i, p in enumerate(prices):
        t = pc_ns(); add2(i, "sell", float(p), 1); dt = pc_ns() - t
        rnd.setdefault((i + 1).bit_length(), []).append(dt)
    del b2
    # drain from the top: ids max_levels-1 ... 0 are best-first
    drain_cancel, drain_best = {}, {}
    for i in range(max_levels - 1, -1, -1):
        L = i + 1
        t = pc_ns(); cancel(i); dc = pc_ns() - t
        t = pc_ns(); ba(); db = pc_ns() - t
        key = L.bit_length()
        drain_cancel.setdefault(key, []).append(dc)
        drain_best.setdefault(key, []).append(db)

    def summarize(d):
        return {str(2 ** (k - 1)): stats(v) for k, v in sorted(d.items()) if len(v) >= 64}

    return {"insert_new_best": summarize(ins), "insert_random_price": summarize(rnd),
            "cancel_top": summarize(drain_cancel), "best_ask_after_cancel": summarize(drain_best)}


def run_sweep(ks: list[int], levels: int = 2048, reps: int = 300) -> dict:
    """Market buy that consumes exactly k ask levels (1 order of qty 1 each).
    Book rebuilt (untimed) before every trial."""
    out = {}
    for k in ks:
        lat = []
        for r in range(reps):
            b = OrderBook()
            for i in range(levels):
                b.add(i, "sell", float(1000 + i), 1)
            t = pc_ns(); fills = b.add(10**9, "buy", None, k); dt = pc_ns() - t
            assert len(fills) == k
            lat.append(dt)
        out[str(k)] = stats(lat)
    return out


def main():
    env = {"python": platform.python_version(), "impl": platform.python_implementation(),
           "machine": platform.machine(), "platform": platform.platform(),
           "timer_overhead_ns": OVH, "repeats": REPEATS}
    print(env, flush=True)
    results = {"env": env, "passive": {}, "mixed": {}}
    for n in SIZES:
        results["passive"][str(n)] = r = run_passive(n)
        print(f"passive N={n:>9,}: {r['throughput_ops_s']:,.0f} ops/s  "
              f"mean={r['add']['mean_ns']:.0f}ns p99={r['add']['p99_ns']}ns", flush=True)
    for n in SIZES:
        results["mixed"][str(n)] = r = run_mixed(n)
        print(f"mixed   N={n:>9,}: {r['throughput_ops_s']:,.0f} ops/s  " +
              "  ".join(f"{k}={v['mean_ns']:.0f}/{v['p99_ns']}" for k, v in r["ops"].items()),
              flush = True)
    results["gc_effect_1M" if not QUICK else "gc_effect"] = run_gc_effect(SIZES[-1])
    print("gc:", {k: (round(v["mean_ns"]), v["p99_ns"], v["max_ns"])
                  for k, v in results["gc_effect_1M" if not QUICK else "gc_effect"].items()}, flush = True)
    results["level_scaling"] = run_level_scaling(2 ** 14 if QUICK else 2 ** 20)
    print("level scaling done", flush = True)
    results["sweep"] = run_sweep([1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024],
                                 reps=50 if QUICK else 300)
    print("sweep done", flush=True)
    with open("results.json", "w") as f:
        json.dump(results, f, indent = 1)

if __name__ == "__main__":
    main()