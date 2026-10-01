# STT_11

Budget invested: $1,000,000

- 🏛 Exchange license — your venue is assigned port 8767

- 🏦 `stt_11_broker_1` ($300,000):

      TEAM_ID=stt_11_broker_1 python -m team.broker

- 🏦 `stt_11_broker_2` ($100,000):

      TEAM_ID=stt_11_broker_2 python -m team.broker

- 🤖 `stt_11_trader_1` ($100,000):

      TEAM_ID=stt_11_trader_1 python -m team.trader

- 🤖 `stt_11_trader_2` ($100,000):

      TEAM_ID=stt_11_trader_2 python -m team.trader

- 🤖 `stt_11_trader_3` ($100,000):

      TEAM_ID=stt_11_trader_3 python -m team.trader

## Where to write code

- `trader.py` → `MyTrader.on_tick()` — your edge
- `broker.py` → `MyBroker.spread()/skew()` — quoting and inventory
- `exchange.py` → `MyExchange` — your fee schedule and policy
- `config.py` → your tunables

Each class derives an `arena` base that handles all plumbing —
see `from arena import Trader, Broker, Exchange`.
