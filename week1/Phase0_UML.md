#Phase 0 UML Diagram 

```mermaid 
classDiagram
    class Trader{
        <<abstract>>
        on_tick(market, portfolio)
        on_fill(side, symbol, quantity)
        on_event(event, message, data)
        on_ipo(symbol, lo, high, shares, data)
        run()

    }
    class MyTrader {
        +on_tick(market, portfolio) override
        +on_fill(side, symbol, quantity, price) override
        +on_event(event, message, data) override
        +on_ipo(symbol, lo, hi, shares, data) override
        +run() override
        -_trade_starter(market, portfolio) new
        -_trade_shock(market, portfolio) new
        -_shock_order(s, sym, side, qty, price, why) new
        -_end_shock() new
    }
    class TraderBot {
        <<SDK runtime>>
        +market: MarketData
        +portfolio: Portfolio
        +connect()
        +run()
        #_place_order(signal)
        #_on_session_event(msg)
    }
    class _AdaptedTraderBot {
        <<SDK adapter>>
        #_owner: Trader
        #_on_trade(msg) override
        #_on_session_event(msg) override
    }
    class TakerBot {
        +connect() override
        #_place_order(signal) override
        #_on_session_event(msg) override
        #_on_ack(raw) new
        +run() inherited
    }
    class MarketData {
        +symbols() list[str]
        +mid_price(symbol) float | None
        +best_bid(symbol) float | None
        +best_ask(symbol) float | None
        +update_book(snapshot)
    }
    class Portfolio {
        +cash: float
        +positions: dict[str, int]
        +can_buy(symbol, qty, price) bool
        +can_sell(symbol, qty) bool
    }
    class Signal {
        +symbol: str
        +side: str
        +quantity: int
        +price: float
    }
    class Broker {
        <<base class>>
        +spread(symbol, price, history) float | None
        +skew(symbol, inventory) float | None
        +toxic(trader_id) bool
        +on_fill(side, symbol, quantity, price)
        +run()
        #_run_venues()
    }
    class MyBroker {
        +spread(symbol, price, history) override
        +skew(symbol, inventory) override
        #_run_venues() override
        +toxic(trader_id) inherited
        +on_fill(side, symbol, quantity, price) inherited
        +run() inherited
    }
    class BrokerBot {
        <<SDK runtime>>
        +connect_to_exchange()
        +quote_symbol(symbol)
        +cancel_quotes(symbol)
        +requote_loop()
        +run()
        #_send(msg)
        #_on_order_ack(msg)
    }
    class _AdaptedBrokerBot {
        <<SDK adapter>>
        #_owner: Broker
        #_on_trade(msg) override
    }
    class BudgetedBrokerBot {
        +connect_to_exchange() override
        +quote_symbol(symbol) override
        +requote_loop() override
        #_send(msg) override
        #_room() new
        #_requote_cost(symbol) new
        #_urgency(symbol, centre) new
        +run() inherited
    }
    class Exchange {
        <<base class>>
        +taker_bps: float | None
        +rebate_bps: float | None
        +accept_order(order, portfolio) tuple
        +on_trade(symbol, price, quantity, buyer_id, seller_id)
        +run()
    }
    class MyExchange {
        +taker_bps = 14
        +rebate_bps = 11
        +accept_order(order, portfolio) inherited
        +on_trade(symbol, price, quantity, buyer_id, seller_id) inherited
        +run() inherited
    }
    class handshake {
        <<message>>
        team_id: str
        role: str
        level: int
        token: str
    }
    class place_order {
        <<message>>
        team_id: str
        symbol: str
        side: str
        order_type: str
        price: float
        quantity: int
    }
    class cancel_order {
        <<message>>
        team_id: str
        order_id: str
        symbol: str
    }
    class order_ack {
        <<message>>
        order_id: str
        team_id: str
        symbol: str
        side: str
        price: float
        quantity: int
    }
    class book_snapshot {
        <<message>>
        symbol: str
        bids: list
        asks: list
        mid_price: float
        spread: float
    }

    Trader <|-- MyTrader
    TraderBot <|-- _AdaptedTraderBot
    _AdaptedTraderBot <|-- TakerBot
    Broker <|-- MyBroker
    BrokerBot <|-- _AdaptedBrokerBot
    _AdaptedBrokerBot <|-- BudgetedBrokerBot
    Exchange <|-- MyExchange

    TraderBot *-- MarketData : owns
    TraderBot *-- Portfolio : owns
    _AdaptedTraderBot o-- Trader : calls hooks on
    _AdaptedBrokerBot o-- Broker : calls hooks on
    MyTrader ..> TakerBot : run() starts
    MyBroker ..> BudgetedBrokerBot : _run_venues() starts
    Trader ..> Signal : on_tick returns

    TakerBot ..> handshake : sends (connect)
    TakerBot ..> place_order : sends IOC (_place_order)
    BudgetedBrokerBot ..> handshake : sends (connect_to_exchange)
    BudgetedBrokerBot ..> place_order : sends quotes
    BudgetedBrokerBot ..> cancel_order : sends (requote)
    MyExchange ..> order_ack : sends to trader / broker
    MyExchange ..> book_snapshot : sends to trader / broker
    order_ack ..> TakerBot : read by _on_ack
    order_ack ..> BrokerBot : read by _on_order_ack
    book_snapshot ..> MarketData : stored by update_book
```

#inheritance  

1. MyTrader: {base class: arena.Trader, overrides: on_tick, on_fill, on_event, on_ipo, run, added: _trader_starter, _trade_shock, _shock_order, _end_shock}
2. TakerBot: {base class: _AdaptedTraderBot which comes from TraderBot, overrides:connect, _place_order, _on_session_event, added: run, listen, trading_loop, _on_ack}
3. MyBroker:{base class: arean.Broker, overrides: spread, skew, _run_venues}
4. BudgetedBrokerBot: {Base_class: _AdaptedBrokerBot which comes from BrokerBot, overrides: connect_to_exchange, quote_symbol, requote_loop, _send, added: _room, _requote_cost, _urgency}
5. MyExchange: {base_class: arena.Exchange, overrides: taker_bps, rebate_bps}

    Additional
    - MyTrader.run() starts TakerBot instead of the SDK's default bot and
    - MyBroker._run_venues() starts BudgetedBrokerBot

#messaging between classes 
1. handshake: sent by TakerBot.connect(), recieved by BudgetedBrokerBot.connect_to_exchange(), to authenticate seat 
2. place_order: sent by TakerBot._place_order(), and BudgetedBrokerBot, received by exchange, to place an order
3. cancel_order: sent by BudgetedBrokerBot when it cancels or requotes an order, receieved by exchange, to cancel an order
4. order_ack: sent by exchange, received by TakerBot._on_ack(), and BrokerBot.on_order_ack(), to confirm an order and return its id
5. book_snapshot: sent by exchange, received by MarketData.update_book(), to get order book for a symbol

