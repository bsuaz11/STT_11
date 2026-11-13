import json
import math

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

R = json.load(open("results.json"))
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, MUTED, GRID, SURF = "#1f1f1e", "#6b6a64", "#e4e3de", "#fcfcfb"

plt.rcParams.update({
    "figure.facecolor": SURF, "axes.facecolor": SURF, "axes.edgecolor": GRID,
    "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False,
    "font.size": 10, "axes.titlesize": 11, "axes.titlecolor": INK,
    "legend.frameon": False, "legend.labelcolor": INK,
})


def line(ax, xs, ys, color, label):
    ax.plot(xs, ys, color=color, lw=2, marker="o", ms=5, label=label,
            markeredgecolor=SURF, markeredgewidth=1.5)


def linfit(xs, ys):
    n = len(xs); mx = sum(xs) / n; my = sum(ys) / n
    b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sum((x - mx) ** 2 for x in xs)
    a = my - b * mx
    ss_res = sum((y - a - b * x) ** 2 for x, y in zip(xs, ys))
    ss_tot = sum((y - my) ** 2 for y in ys)
    return a, b, 1 - ss_res / ss_tot


fig, axes = plt.subplots(1, 3, figsize=(15, 4.4))

# (a) mixed workload: p50 latency vs N  -> should be flat (no N term)
ax = axes[0]
Ns = sorted(R["mixed"], key=int)
x = [int(n) for n in Ns]
for key, c, lab in (("add_rest", BLUE, "add (no match)"),
                    ("cancel", ORANGE, "cancel"),
                    ("best_bid", AQUA, "best_bid")):
    line(ax, x, [R["mixed"][n]["ops"][key]["p50_ns"] for n in Ns], c, lab)
ax.set_xscale("log"); ax.set_ylim(bottom=0)
ax.set_xlabel("operations in run (N)"); ax.set_ylabel("p50 latency (ns)")
ax.set_title("(a) Mixed flow: latency vs N")
ax.legend(loc="center right")

# (b) adversarial heap: p50 vs L  -> O(log L) predicted
ax = axes[1]
ls = R["level_scaling"]
Ls = sorted(ls["insert_new_best"], key=int)
x = [int(L) for L in Ls]
line(ax, x, [ls["insert_new_best"][L]["p50_ns"] for L in Ls], BLUE, "add creating new best level")
line(ax, x, [ls["insert_random_price"][L]["p50_ns"] for L in Ls], ORANGE, "add at random new level")
line(ax, x, [ls["best_ask_after_cancel"][L]["p50_ns"] for L in Ls], AQUA, "best_ask after cancelling top")
ax.set_xscale("log", base=2); ax.set_ylim(bottom=0)
ax.set_xlabel("live price levels on side (L)"); ax.set_ylabel("p50 latency (ns)")
ax.set_title("(b) Heap operations vs L")
ax.legend(loc="upper left", fontsize=9)
ax.set_ylim(0, 2800)

# (c) sweep: mean vs k (log-log) -> slope 1 predicted
ax = axes[2]
ks = sorted(R["sweep"], key=int)
kx = [int(k) for k in ks]
ky = [R["sweep"][k]["p50_ns"] / 1000 for k in ks]
line(ax, kx, ky, BLUE, "market order sweeping k levels (p50)")
a, b, r2 = linfit(kx, ky)
ax.plot(kx, [a + b * k for k in kx], color=MUTED, lw=1, ls="--",
        label=f"linear fit: {a:.1f} + {b:.2f}·k µs  (R²={r2:.4f})")
ax.set_xscale("log", base=2); ax.set_yscale("log")
ax.set_xlabel("price levels swept (k)"); ax.set_ylabel("p50 latency (µs)")
ax.set_title("(c) Sweep cost vs k  (L = 2048)")
ax.legend(loc="upper left")

fig.tight_layout()
fig.savefig("bench_scaling.png", dpi=150)

# fits for the write-up
lx = [math.log2(int(L)) for L in Ls]
for key in ("insert_new_best", "insert_random_price", "best_ask_after_cancel"):
    a2, b2, r = linfit(lx, [ls[key][L]["p50_ns"] for L in Ls])
    print(f"{key:24s} p50 ≈ {a2:6.0f} + {b2:5.1f}·log2(L) ns   R²={r:.2f}")
print(f"sweep p50 ≈ {a:.2f} + {b:.3f}·k µs   R²={r2:.5f}")
