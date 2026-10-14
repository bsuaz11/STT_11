import asyncio
import json
import logging
import websockets
import yfinance as yf

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("client")

#redials after a failure, waiting a bit longer each time (exponential backoff).
async def consume(url: str) -> None:
    delay = 1.0
    while True:
        try:
            #dial the server
            async with websockets.connect(url) as ws:
                delay = 1.0
                #receive messages one at a time and pass each to handle.
                async for raw in ws:
                    handle(raw)          # must not raise
        except websockets.exceptions.ConnectionClosed as e:
            #the server dropped the call
            log.warning("connection closed: %r", e)  # log, then retry
        except asyncio.TimeoutError:
            log.warning("timed out") 
        except OSError as e:
            log.warning("could not connect: %r", e)
        log.info("retrying in %.0fs", delay)
        await asyncio.sleep(delay)
        delay = min(delay * 2, 30.0)

#Handle function with one message and never crashes
def handle(raw):
    try:
        msg = json.loads(raw)        # turn text into a dict
        price = msg["price"]         # get the price out of it
        log.info("got price %s", price)
    except Exception as e:
        log.warning("bad message %r, dropping it (%r)", raw, e)

#json.loads(raw) fails if the text is not valid JSON
#msg["price"] fails if the dict has no "price" key.
#except Exception catches both failures, logs them, and the function ends normally. 
# Because it ends normally, the async for loop moves on to the next message.