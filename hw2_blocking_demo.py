#WHAT THIS FILE SHOWS
#asyncio runs every coroutine on ONE thread (the event loop). The loop can only switch to another task
#when the running coroutine hits an `await`. A blocking call (time.sleep, a normal HTTP get, yfinance)
#has no `await`, so it freezes the loop and EVERY other task waits, e.g. the broker's websocket reads/heartbeats.
#The fix is to run the blocking call on a helper thread with asyncio.to_thread, so the loop stays free.
#
#HOW WE MEASURE IT (numbers, not vibes)
#3 "other tasks" each try to wake up every 100 ms. Each wake-up records how LATE it was.
#A 4th task (the "poller") makes one 2-second slow call. We try it 4 ways (2 wrong, 2 right) and compare
#how late the other tasks were. We also count asyncio's debug-mode "took X seconds" warnings.

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
#this lets us test a REAL blocking HTTP get, not just time.sleep, with a predictable delay
class SlowHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        time.sleep(BLOCK_FOR)    # pretend Yahoo is slow
        body = b'{"price": 187.42}'
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass                     # silence the server's per-request log lines


#port 0 = let the OS pick a free port. The server runs on its own daemon thread,
#completely separate from the asyncio event loop we are testing.
server = ThreadingHTTPServer(("127.0.0.1", 0), SlowHandler)
threading.Thread(target=server.serve_forever, daemon=True).start()
URL = f"http://127.0.0.1:{server.server_port}/"


#a normal (synchronous) HTTP get, the same kind of call yfinance makes under the hood.
#it does not `await` anything, so if called directly in a coroutine it blocks the event loop until the reply arrives.
def blocking_http_get():
    with urllib.request.urlopen(URL) as r:
        return r.read()


#count asyncio's slow-callback warnings (only emitted in debug mode)
#with debug=True, asyncio logs "Executing <Task ...> took X seconds" whenever one step
#holds the loop longer than loop.slow_callback_duration (default 0.1 s). That is our detector.
class CountWarnings(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        if "took" in record.getMessage():
            self.messages.append(record.getMessage())


counter = CountWarnings()
logging.getLogger("asyncio").addHandler(counter)
logging.getLogger("asyncio").propagate = False   # collect the warnings here instead of printing them raw


#an "other task" (stand-in for websocket reads/heartbeats): records how late every wake-up is
#wake-up i is scheduled for start + i*PERIOD. lateness = when it actually ran - when it was scheduled.
#if the loop is healthy, lateness is a few ms. If something froze the loop, lateness jumps.
async def ticker(lateness: list) -> None:
    start = time.perf_counter()
    i = 0
    while True:
        i += 1
        target = start + i * PERIOD
        if target - start > RUN_FOR:
            return
        #`await` hands control back to the loop so other tasks can run while we wait
        await asyncio.sleep(max(0.0, target - time.perf_counter()))
        lateness.append(time.perf_counter() - target)


#the four pollers: two WRONG (block the loop), two RIGHT (offload to a thread)
#each one waits 0.5 s first (so the tickers are already running), then makes the 2 s slow call.

#WRONG: time.sleep has no `await`. The loop's only thread sits here for 2 s, so no other task can run.
async def wrong_sleep():
    await asyncio.sleep(0.5)
    time.sleep(BLOCK_FOR)


#WRONG: same problem with a real HTTP get. This is what calling yfinance directly in a coroutine does.
async def wrong_http():
    await asyncio.sleep(0.5)
    blocking_http_get()


#RIGHT: to_thread runs time.sleep on a helper thread. `await` lets the loop keep serving the other tasks
#until the helper finishes.
async def right_sleep():
    await asyncio.sleep(0.5)
    await asyncio.to_thread(time.sleep, BLOCK_FOR)


#RIGHT: same fix for the HTTP get. This is how the broker should poll yfinance.
async def right_http():
    await asyncio.sleep(0.5)
    await asyncio.to_thread(blocking_http_get)


#run one poller alongside 3 tickers and summarize how late the tickers were
async def experiment(poller) -> dict:
    lates = [[], [], []]
    t0 = time.perf_counter()
    #gather runs all 4 tasks "at the same time" on the one event loop
    await asyncio.gather(poller(), *(ticker(l) for l in lates))
    wall = time.perf_counter() - t0
    all_ms = [x * 1000 for l in lates for x in l]
    return {
        #total runtime: about 3 s for EVERY version, so a single stopwatch would NOT reveal the bug
        "wall": wall,
        "ticks": len(all_ms),
        #typical lateness of a wake-up
        "median": statistics.median(all_ms),
        #worst lateness: about the full 2 s when the loop was frozen
        "max": max(all_ms),
        #how many wake-ups missed their slot by more than 100 ms
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
        #debug=True turns on the slow-callback detector. The WRONG versions should trigger it, the RIGHT ones should not.
        r = asyncio.run(experiment(poller), debug=True)
        print(f"{name:<34}{r['wall']:>7.2f}{r['median']:>16.1f}{r['max']:>13.1f}"
              f"{r['late100']:>13}{len(counter.messages):>13}")
        #print the warning asyncio gave us: it names the guilty coroutine and how long it held the loop
        for m in counter.messages:
            print("    asyncio says:", m.split(" result=")[0].replace("Executing ", "") + " took" + m.rsplit("took", 1)[1])


#EXPECTED RESULT
#WRONG rows: max lateness ~2000 ms, ~60 of 90 wake-ups >100 ms late, 1 debug warning naming the coroutine.
#  (loop frozen from t=0.5 to t=2.5 s -> 20 missed slots per ticker x 3 tickers = 60)
#RIGHT rows: max lateness a few ms, 0 late wake-ups, 0 warnings.
if __name__ == "__main__":
    main()
