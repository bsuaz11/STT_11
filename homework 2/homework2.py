import asyncio
import websockets
import time

#producer adds items to the back of queue 
#consumer takes items from the front of the queue 

async def producer(q,n_ticks):
    count = 0
    peak = 0
    for i in range(n_ticks):
        await q.put(i)
        count += 1
        peak = max(peak, q.qsize())  
    return count, peak
        

async def consumer(q, consumer_id, seen):
    while True:
        tick = await q.get()
        try:
            seen.append(tick)
            await asyncio.sleep(0)
        finally:
            q.task_done()

async def main(n_consumers: int = 3, n_ticks: int = 1000, maxsize = 1) -> dict:
    """Run the pipeline; return {"produced": int, "consumed": int, "elapsed_s": float}."""
    #set up queue (maxsize 1 or 10,000)
    q = asyncio.Queue(maxsize = maxsize)
    seen = []
    start = time.perf_counter()
    #start the consumers 
    consumers = [asyncio.create_task(consumer(q, i, seen)) for i in range (n_consumers)]
    
    #get the producer
    produced, peak = await producer(q,n_ticks)
    #wait for queue to be done 
    await q.join()
    #shut down 
    for c in consumers:
        c.cancel()
    results = await asyncio.gather(*consumers, return_exceptions= True)
    elapsed_s = time.perf_counter() - start

    #check to make sure nothing llost 
    assert set(seen) == set(range(n_ticks))
    #check to make sure nothing double processed
    assert len(set(seen)) == len(seen)
    #return 
    return {
        "produced": produced,
        "consumed": len(seen),
        "elapsed_s": elapsed_s,
        "peak_depth": peak,
        "throughput": len(seen) / elapsed_s
    }

if __name__ == "__main__":
    print(asyncio.run(main()))

    for size in (1, 10_000):
        r = asyncio.run(main(n_ticks=100_000, maxsize=size))
        print(f" size:{size} , elapsed: {r['elapsed_s']}, throughput: {r['throughput']}, peak depth: {r['peak_depth']}")

    


# async def consume(url: str) -> None:
#     delay = 1.0
#     while True:
#         try:
#             async with websockets.connect(url) as ws:
#                 delay = 1.0
#                 async for raw in ws:
#                     handle(raw)          # must not raise
#         except websockets.exceptions.ConnectionClosed:
#             ...                          # log, then retry
#         except asyncio.TimeoutError:
#             ...
#         await asyncio.sleep(delay)
#         delay = min(delay * 2, 30.0)