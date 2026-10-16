1. table for backpressures
    size:1, elapsed: 2.44385, throughput: 40919.07239, peak depth: 1
    size: 1000, elapsed: 1.14900, throughput: 87032.13564, peak depth:10000
For screenshot of table, see screenshot in STT_11/homework2/screenshot in git repo 


2. Write a 200-word explanation: why does the broker's yfinance polling run on a separate thread (e.g. asyncio.to_thread or an explicit worker thread) while all socket I/O stays on the event loop? Name the concrete failure mode of calling blocking HTTP inside a coroutine, and how you would detect it (asyncio.run(..., debug=True) / slow-callback warnings).

An asyncio program runs every coroutine on a single thread, the event loop. So, the thread only makes progress when the running coroutine reaches an await, allowing the event loop to "move on" to another coroutine/task.
yfinance is synchronous: it makes a blocking HTTP request to Yahoo and never allows the program to move on to other coroutines. If the broker calls it directly inside a coroutine, the entire event loop freezes for the full duration of the request. During that freeze, no other task runs. Incoming websocket messages go unread, outgoing quotes aren't sent, and timers fire late. If the freeze outlasts the websocket ping timeout (20 seconds by default in websockets), the keepalive fails and the connection is closed as dead. The broker then reconnects and quotes stale prices (not good!).
To prevent this, the polling is moved off the loop with await asyncio.to_thread(...) or a dedicated worker thread that hands results back with loop.call_soon_threadsafe. The blocking wait happens on another OS thread while the loop keeps servicing sockets. Socket I/O stays on the loop because the websocket's library is async-native, non-blocking, and not thread-safe. Asyncio's transports and queues must only be touched by the loop's thread.
To detect the bug, run asyncio.run(main(), debug=True) (or set PYTHONASYNCIODEBUG=1). Asyncio then logs "Executing <Task …> took 2.005 seconds" for any step that holds the loop longer than slow_callback_duration (0.1 s by default), which identifies which coroutine froze the loop.

For wrong version, see file hw2_blocking_demo.py