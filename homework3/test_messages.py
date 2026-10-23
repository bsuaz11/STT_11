import pytest
from pydantic import TypeAdapter, ValidationError
from messages import (Handshake, CancelOrder, PlaceOrder, 
                      OrderAck, BookSnapshot, Message)
ADAPTER = TypeAdapter(Message) 
#turns the union into smth with a .validate_json() method

def test_handshake_round_trip():
    m = Handshake(team_id="t1", role="trader", level=2, token="abc")
    assert Handshake.model_validate_json(m.model_dump_json()) == m

def test_cancelorder_round_trip():
    m = CancelOrder(team_id="t1", order_id="o12", symbol="abc")
    assert CancelOrder.model_validate_json(m.model_dump_json()) == m

#PlaceOrder is the one model with two different shapes:
#limit order: has a price, market order: no price (price=None)
#None is the value likely to get lost when converting to JSON because it gets
#written as null, and has to come as None, not 0.0 nor missing. 
# Write two tests, one for each shape
def test_placeorder_limit_round_trip():
    m = PlaceOrder(team_id="t1", symbol="AAPL", side="buy", order_type="limit", 
                   price=101.5, quantity=10)
    assert PlaceOrder.model_validate_json(m.model_dump_json()) == m
#used a decimal for price b/c more noticeable if it got lost

def test_placeorder_market_round_trip():
    m = PlaceOrder(team_id="t1", symbol="AAPL", side="sell", 
                   order_type="market", price=None, quantity=10)
    back = PlaceOrder.model_validate_json(m.model_dump_json())
    assert back == m
    assert back.price is None

# 1. A normal limit order resting in the book: has a real price and queue
# position, queue_ahead (shares in line ahead of you) and level_qty (total
# shares at that price)
# 2. A market-on-close ack: the server sends price=0.0 b/c there's no limit price
# this is the case allowed when we changed gt=0 to ge=0

# 1: Test a resting limit order -> Check that all fields survive the trip
def test_orderack_resting_round_trip():
    m = OrderAck(order_id="o42", team_id="t1", symbol="AAPL", side="buy",
                price=101.5, quantity=10, queue_ahead=300, level_qty=450)
    assert OrderAck.model_validate_json(m.model_dump_json()) == m

# 2: Check that the edge case (price=0.0) works
def test_orderack_zeroprice_round_trip():
    m = OrderAck(order_id="o42", team_id="t1", symbol="AAPL", side="sell",
                price=0.0, quantity=10)
    back = OrderAck.model_validate_json(m.model_dump_json())
    assert back == m
    assert back.price == 0.0

#BookSnapshot model uses the tuple levels (list[Level] with Positive Float)
#test should check that those survive the trip 
# Two shapes: 1)Normal, full book: bids and asks on both sides, checks that
# every field survives, 2)Empty book: no bids, no asks: can happen 
# (start of a session or after big sweep)

# 1: Test a full book
def test_booksnapshot_full_round_trip():
    m = BookSnapshot(symbol="AAPL", 
                    bids=[(99.5, 100.0), (99.4, 50.0)], #best (highest) first
                    asks=[(100.5, 80.0), (100.6, 120.0)], #best (lowest) first
                    mid_price=100.0, spread=1.0, ref_price=100.02, #mid=(99.5+100.5)/2
                    asset_type="future", microprice=99.95, obi=-0.14)
    #asset type is just not the default "equity"
    #all prices are unqiue so errors can be detected
    back = BookSnapshot.model_validate_json(m.model_dump_json())
    assert back == m
    assert back.bids[0] == (99.5, 100.0) #in JSON, a level is written as a list
    #when parsed back, check that turns back into a tuple (price, qty)
    #Two levels per side, in the corect order (bids highest to lowest, asks opposite)
    #this catches if levels got swapped between bids and asks
    #mid_price and spread agree with the levels 

def test_booksnapshot_empty_round_trip():
    m = BookSnapshot(symbol="AAPL",
                    bids=[], asks=[],
                    mid_price=100.0, spread=0.0, ref_price=100.0,
                    asset_type="future", microprice=100.0, obi=0.0)
    #these numbers correspond to what the exchange sends when the book is empty
    back = BookSnapshot.model_validate_json(m.model_dump_json())
    assert back == m
    assert back.bids == [] and back.asks == []

#Dispatch: the adapter has to pick the class from the "type" tag alone
def test_dispatch_cancel_order():
    raw = '{"type":"cancel_order","team_id":"t1","order_id":"o1","symbol":"AAPL"}'
    msg = ADAPTER.validate_json(raw)
    assert isinstance(msg, CancelOrder)
    assert msg.order_id == "o1"
#raw is plain text, like what arrives over the WebSocket
#only the "type" valye inside the string says "CancelOrder"
#isinstance checks that the sorter put it in the right slot
#msg.order_id=="o1" checks that the fields were filled in 

def test_dispatch_book_snapshot():
    raw = '{"type":"book_snapshot","symbol":"AAPL","bids":[[99.5,100]],"asks":[[100.5,80]],"mid_price":100.0,"spread":1.0}'
    msg = ADAPTER.validate_json(raw)
    assert isinstance(msg, BookSnapshot)
    assert msg.bids[0] == (99.5, 100.0) #check the sorter applied Level Type not just class
    assert msg.ref_price == 0.0 #the default should be filled in 

#Instead of writing five more tests, use one test that pytest runs once
# per message
#SAMPLES holds one valid example of every message type and both kinds of PlaceOrder
SAMPLES = [
    Handshake(team_id="t1", role="trader", level=2, token="abc"),
    PlaceOrder(team_id="t1", symbol="AAPL", side="buy", order_type="limit",
               price=101.5, quantity=10),
    PlaceOrder(team_id="t1", symbol="AAPL", side="sell", order_type="market",
               price=None, quantity=10),
    CancelOrder(team_id="t1", order_id="o12", symbol="AAPL"),
    OrderAck(order_id="o42", team_id="t1", symbol="AAPL", side="buy",
             price=101.5, quantity=10, queue_ahead=300, level_qty=450),
    BookSnapshot(symbol="AAPL", bids=[(99.5, 100.0)], asks=[(100.5, 80.0)],
                 mid_price=100.0, spread=1.0),
]

#run the test below once for each item in SAMPLES, and pass that item in as m
#the @ line is a decorator: it attaches extra behavior to the function under it
#the result is one function and 6 test runs, each reported separately 
#m.model_dump_json() serializes using the specific model
#ADAPTER.validate_json(...) parses using the union 
#type(back) is type(m): checks that it's exactly the same class, stricter
#back == m checks that nothing was lost along the way
@pytest.mark.parametrize("m", SAMPLES)
def test_dispatch_every_model(m):
    back = ADAPTER.validate_json(m.model_dump_json())
    assert type(back) is type(m)
    assert back == m 

# Negative tests: bad payloads must be rejected, and for the right reason.
# Each payload is valid except for the ONE thing being tested, and we assert
# on the error's type code and location (not just that *some* error happened).
def test_reject_quantity_zero():
    raw = '{"type":"place_order","team_id":"t1","symbol":"AAPL","side":"buy","quantity":0}'
    with pytest.raises(ValidationError) as exc:
        ADAPTER.validate_json(raw)
    errors = exc.value.errors()
    assert len(errors) == 1
    assert errors[0]["type"] == "greater_than"
    assert errors[0]["loc"] == ("place_order", "quantity")

# missing field: a cancel_order with no order_id
def test_reject_missing_field():
    raw = '{"type":"cancel_order","team_id":"t1","symbol":"AAPL"}'
    with pytest.raises(ValidationError) as exc:
        ADAPTER.validate_json(raw)
    errors = exc.value.errors()
    assert len(errors) == 1
    assert errors[0]["type"] == "missing"
    assert errors[0]["loc"] == ("cancel_order", "order_id")

# wrong type: quantity is text that can't be read as a number
def test_reject_wrong_type():
    raw = '{"type":"place_order","team_id":"t1","symbol":"AAPL","side":"buy","quantity":"ten"}'
    with pytest.raises(ValidationError) as exc:
        ADAPTER.validate_json(raw)
    errors = exc.value.errors()
    assert len(errors) == 1
    assert errors[0]["type"] == "int_parsing"
    assert errors[0]["loc"] == ("place_order", "quantity")

# unknown tag: the sorter fails before picking a model, so loc is empty
def test_reject_unknown_type_tag():
    raw = '{"type":"pizza","team_id":"t1"}'
    with pytest.raises(ValidationError) as exc:
        ADAPTER.validate_json(raw)
    errors = exc.value.errors()
    assert len(errors) == 1
    assert errors[0]["type"] == "union_tag_invalid"
    assert errors[0]["loc"] == ()
    assert "pizza" in str(exc.value)
