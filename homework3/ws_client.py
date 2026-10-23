"""Connect, handshake, then print the top 5 book levels for 30 seconds.

Run:  python ws_client.py   (EXCHANGE_URL and ARENA_TOKEN come from the
repo's .env, like the bots; shell variables win)
"""
import asyncio
import os
import pathlib
from itertools import zip_longest

import websockets
from pydantic import TypeAdapter, ValidationError

from messages import BookSnapshot, Handshake, Message

ENV = pathlib.Path(__file__).resolve().parent.parent / ".env"
for line in ENV.read_text().splitlines() if ENV.exists() else []:
    key, sep, value = line.partition("=")
    if sep and not key.strip().startswith("#"):
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))

URL = os.environ.get("EXCHANGE_URL", "ws://localhost:8765")
ADAPTER = TypeAdapter(Message)    # the union: raw JSON -> the right model, no dicts
DEPTH, SECONDS = 5, 30


def show(book: BookSnapshot) -> None:
    print(f"\n{book.symbol}  mid {book.mid_price:.2f}  spread {book.spread:.2f}")
    print(f"{'bid qty':>8} {'bid':>9} | {'ask':<9} {'ask qty':<8}")
    for bid, ask in zip_longest(book.bids[:DEPTH], book.asks[:DEPTH]):
        b = f"{bid[1]:>8.0f} {bid[0]:>9.2f}" if bid else " " * 18
        a = f"{ask[0]:<9.2f} {ask[1]:<8.0f}" if ask else ""
        print(f"{b} | {a}")


async def main() -> None:
    skipped = 0
    async with websockets.connect(URL) as ws:
        hs = Handshake(team_id="stt_11_observer", role="observer",
                       level=1, token=os.environ.get("ARENA_TOKEN", ""))
        await ws.send(hs.model_dump_json())
        try:
            async with asyncio.timeout(SECONDS):  # fires even if the feed goes quiet
                async for raw in ws:
                    try:
                        msg = ADAPTER.validate_json(raw)
                    except ValidationError:   # a type outside our union (leaderboard, …)
                        skipped += 1
                        continue
                    if isinstance(msg, BookSnapshot):
                        show(msg)
        except TimeoutError:
            pass
    print(f"\nDone after {SECONDS}s ({skipped} frames outside the union skipped)")


if __name__ == "__main__":
    asyncio.run(main())
