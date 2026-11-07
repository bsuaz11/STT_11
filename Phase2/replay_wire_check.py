#!/usr/bin/env python
"""
Phase2/replay_wire_check.py — the replay evidence, taken off scripts/replay_session.py.

Plays a recording through the course's replay server (over a real local socket,
at high speed) and listens as the dashboard would, printing each scenario event
and every order our trader seats took, in the order they went out on the wire.

    PYTHONPATH=engine:. python Phase2/replay_wire_check.py <recording> [--speed 200]
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
from collections import Counter

import websockets

ROOT = pathlib.Path(__file__).resolve().parent.parent
SHOWN = ("CALENDAR_EVENT", "TRADING_HALT", "TRADING_RESUMED", "MARKET_HALT", "MARKET_RESUMED",
         "SSR_ON", "LIQUIDATION", "IPO_OPEN", "IPO_PRICED", "IPO_LISTED", "SESSION_OPEN",
         "SESSION_CLOSED")
PREFIX = "stt_11_"
QUIET_LIMIT_SEC = 8.0       # the replay has finished when the wire stays silent this long


def plain_copy(path: str, folder: str) -> str:
    """replay_session.py reads plain JSONL; class tapes are gzipped."""
    if not path.endswith(".gz"):
        return path
    out = pathlib.Path(folder) / pathlib.Path(path).name.removesuffix(".gz")
    with gzip.open(path, "rb") as src, open(out, "wb") as dst:
        shutil.copyfileobj(src, dst)
    return str(out)


async def listen(port: int) -> Counter:
    counts: Counter = Counter()
    first_tick_ms = None
    for _ in range(50):                  # the server takes a moment to bind
        try:
            ws = await websockets.connect(f"ws://localhost:{port}", max_size=None)
            break
        except OSError:
            await asyncio.sleep(0.2)
    else:
        raise SystemExit("replay server did not come up")
    async with ws:
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), QUIET_LIMIT_SEC)
            except asyncio.TimeoutError:
                return counts
            msg = json.loads(raw)
            kind = msg.get("type")
            counts[kind] += 1
            if kind == "session_event" and msg.get("event") in SHOWN:
                counts[msg["event"]] += 1
                data = msg.get("data") or {}
                print(f"  {msg['event']:16s} {data.get('symbol') or '':6s} {msg.get('message', '')[:72]}")
            elif kind == "trade_execution":
                taker = msg.get("taker_id") or ""
                if msg.get("buyer_id", "").startswith(PREFIX) or msg.get("seller_id", "").startswith(PREFIX):
                    counts["our_fills"] += 1
                if taker.startswith(PREFIX + "trader"):
                    counts["our_taker_fills"] += 1
                    side = "BUY " if msg.get("buyer_id") == taker else "SELL"
                    print(f"      {taker[-8:]} {side} {msg['quantity']:3d} {msg['symbol']:6s} @ {msg['price']:.2f}")
    return counts


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("recording")
    ap.add_argument("--speed", type=float, default=200.0)
    ap.add_argument("--port", type=int, default=8798)
    args = ap.parse_args()
    with tempfile.TemporaryDirectory() as folder:
        plain = plain_copy(args.recording, folder)
        server = subprocess.Popen(
            [sys.executable, str(ROOT / "scripts" / "replay_session.py"), plain,
             "--speed", str(args.speed), "--port", str(args.port)],
            stdout=subprocess.DEVNULL, stdin=subprocess.DEVNULL)
        try:
            counts = asyncio.run(listen(args.port))
        finally:
            server.terminate()
            server.wait()
    print("\nReceived off the replay socket:", dict(counts))


if __name__ == "__main__":
    main()
