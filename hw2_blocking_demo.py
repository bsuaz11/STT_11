import asyncio
import logging
import statistics
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PERIOD = 0.1      # each "other task" wants to wake up every 100 ms
RUN_FOR = 3.0     # seconds per experiment
BLOCK_FOR = 2.0   # how long the slow call takes


#a fake Yahoo: a local HTTP server that takes 2 s to answer (so results don't depend on the internet)
class SlowHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        time.sleep(BLOCK_FOR)
        body = b'{"price": 187.42}'
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


server = ThreadingHTTPServer(("127.0.0.1", 0), SlowHandler)
threading.Thread(target=server.serve_forever, daemon=True).start()
URL = f"http://127.0.0.1:{server.server_port}/"


def blocking_http_get():
    with urllib.request.urlopen(URL) as r:
        return r.read()


#count asyncio's slow-callback warnings (only emitted in debug mode)
class CountWarnings(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        if "took" in record.getMessage():
            self.messages.append(record.getMessage())


counter = CountWarnings()
logging.getLogger("asyncio").addHandler(counter)
logging.getLogger("asyncio").propagate = False


#an "other task" (stand-in for websocket reads/heartbeats): records how late every wake-up is
async def ticker(lateness: list) -> None:
    start = time.perf_counter()
    i = 0
    while True:
        i += 1
        target = start + i * PERIOD
        if target - start > RUN_FOR:
            return
        await asyncio.sleep(max(0.0, target - time.perf_counter()))
        lateness.append(time.perf_counter() - target)


#the four pollers: two WRONG (block the loop), two RIGHT (offload to a thread)
async def wrong_sleep():
    await asyncio.sleep(0.5)
    time.sleep(BLOCK_FOR)


async def wrong_http():
    await asyncio.sleep(0.5)
    blocking_http_get()


async def right_sleep():
    await asyncio.sleep(0.5)
    await asyncio.to_thread(time.sleep, BLOCK_FOR)


async def right_http():
    await asyncio.sleep(0.5)
    await asyncio.to_thread(blocking_http_get)


async def experiment(poller) -> dict:
    lates = [[], [], []]
    t0 = time.perf_counter()
    await asyncio.gather(poller(), *(ticker(l) for l in lates))
    wall = time.perf_counter() - t0
    all_ms = [x * 1000 for l in lates for x in l]
    return {
        "wall": wall,
        "ticks": len(all_ms),
        "median": statistics.median(all_ms),
        "max": max(all_ms),
        "late100": sum(x > 100 for x in all_ms),
    }


def main():
    expected = 3 * int(RUN_FOR / PERIOD)
    print(f"3 tasks x wake-up every {PERIOD*1000:.0f} ms for {RUN_FOR:.0f} s "
          f"(= {expected} wake-ups); poller makes a {BLOCK_FOR:.0f} s call at t=0.5 s\n")
    print(f"{'poller':<34}{'wall s':>7}{'median late ms':>16}{'max late ms':>13}"
          f"{'>100ms late':>13}{'debug warns':>13}")
    for name, poller in [
        ("WRONG time.sleep(2) in coroutine", wrong_sleep),
        ("WRONG urllib GET in coroutine", wrong_http),
        ("RIGHT to_thread(time.sleep, 2)", right_sleep),
        ("RIGHT to_thread(urllib GET)", right_http),
    ]:
        counter.messages.clear()
        r = asyncio.run(experiment(poller), debug=True)
        print(f"{name:<34}{r['wall']:>7.2f}{r['median']:>16.1f}{r['max']:>13.1f}"
              f"{r['late100']:>13}{len(counter.messages):>13}")
        for m in counter.messages:
            print("    asyncio says:", m.split(" result=")[0].replace("Executing ", "") + " took" + m.rsplit("took", 1)[1])


if __name__ == "__main__":
    main()
