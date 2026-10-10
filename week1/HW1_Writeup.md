# HW 1 Write-up
1. UML
    refer to week1/HW1_UML.md

2. Design justification

    Trader is an ABC (abstract base class) with a single abstract method, on_tick, instead of a function I register, because a trading bot depends on state and on hooks for specific events, not just on one decision. Making on_tick abstract lets the class enforce it. If a subclass forgets on_tick, Python stops immediately with an error, so I don't find out halfway through a live session that my bot has no strategy. The other hooks (on_fill, on_event, on_ipo) already default to doing nothing and run() already handles connecting and reconnecting so I only override what I use. But, this costs me freedom. My bot is tied to the SDK's structure and it is harder to test than a plain function where I can just pass in inputs and check the output. The SDK knows that this is a trade-off because it provides as_signal_fn() to turn a Trader back into a normal function for the simulator.

3. Log excerpt
    Refer to week1/run.log -> it has receipts of both

    ```
    16:32:17  INFO      Connecting to wss://feed.algoarena-st.duckdns.org (EXCHANGE_URL from the shell)
    16:32:18  INFO      Connected to wss://feed.algoarena-st.duckdns.org as stt_11_trader_2 (trader)
    16:32:18  INFO      Event: FEE_SCHEDULE | Venue fee schedule: taker 30.0 bps, maker rebate 5.0 bps | {'port': 8765, 'taker': 0.003, 'rebate': 0.0005, 'net': 0.0025, ...}
    16:32:18  INFO      Session event: SESSION_OPEN — Session already open — you joined mid-session.
    16:34:18  INFO      Order: buy 20 MSFT @ 433.69
    16:34:18  INFO      order_ack -> id: 513dae0b-83fc-4685-9442-3914e0da3814 buy 20 MSFT @ 433.69
    ```

    - The "Connected" line is the handshake
    - the "Event:" line comes from our `on_event` override
    - "order_ack" line shows the exchange accepted the order (level 1)
