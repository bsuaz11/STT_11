import random
import pytest
from orderbook import OrderBook
@pytest.fixture

def book():
    return OrderBook()
# first scan opposite side best price first
def test_buy_sweeps_asks_lowest_price_first(book):
    book.add(1, "sell", 102.0, 5)
    book.add(2, "sell", 100.0, 5)
    book.add(3, "sell", 101.0, 5)
    fills = book.add(9, "buy", 102.0, 12)
    assert fills == [(9, 2, 100.0, 5), (9, 3, 101.0, 5), (9, 1, 102.0, 2)]
    assert book.depth("sell") == [(102.0, 3)]

def test_sell_sweeps_bids_highest_price_first(book):
    book.add(1, "buy", 98.0, 5)
    book.add(2, "buy", 100.0, 5)
    book.add(3, "buy", 99.0, 5)
    fills = book.add(9, "sell", 98.0, 11)
    assert fills == [(9, 2, 100.0, 5), (9, 3, 99.0, 5), (9, 1, 98.0, 1)]


def test_limit_stops_at_its_price(book):
    book.add(1, "sell", 100.0, 5)
    book.add(2, "sell", 101.0, 5)
    fills = book.add(9, "buy", 100.0, 8)  
    assert fills == [(9, 1, 100.0, 5)]
    assert book.best_bid() == 100.0 and book.best_ask() == 101.0
    assert book.depth("buy") == [(100.0, 3)]


def test_non_crossing_order_does_not_trade(book):
    book.add(1, "sell", 101.0, 5)
    assert book.add(2, "buy", 100.0, 5) == []
    assert book.best_bid() == 100.0 and book.best_ask() == 101.0

#2. trade needs to execute at resting order's price
def test_aggressive_buy_gets_resting_price(book):
    book.add(1, "sell", 100.0, 5)
    assert book.add(2, "buy", 105.0, 5) == [(2, 1, 100.0, 5)]

def test_aggressive_sell_gets_resting_price(book):
    book.add(1, "buy", 100.0, 5)
    assert book.add(2, "sell", 90.0, 5) == [(2, 1, 100.0, 5)]

#3. unfilled part of limit order needs to rest at the limit price
def test_limit_remainder_rests(book):
    book.add(1, "sell", 100.0, 3)
    fills = book.add(2, "buy", 101.0, 10)
    assert fills == [(2, 1, 100.0, 3)]
    assert book.best_bid() == 101.0     
    assert book.depth("buy") == [(101.0, 7)]
    assert book.resting_qty(2) == 7
    assert book.best_ask() is None


def test_rested_remainder_is_cancellable_and_matchable(book):
    book.add(1, "sell", 100.0, 3)
    book.add(2, "buy", 100.0, 10)                  # 7 rests
    assert book.add(3, "sell", 100.0, 4) == [(3, 2, 100.0, 4)]
    assert book.resting_qty(2) == 3
    assert book.cancel(2) is True
    assert book.best_bid() is None

#4. market order's remainder is cancelled
def test_market_order_remainder_is_cancelled(book):
    book.add(1, "sell", 100.0, 3)
    book.add(2, "sell", 105.0, 2)
    fills = book.add(9, "buy", None, 10) 
    assert fills == [(9, 1, 100.0, 3), (9, 2, 105.0, 2)]
    assert book.best_bid() is None 
    assert book.best_ask() is None
    assert 9 not in book and len(book) == 0
    assert book.cancel(9) is False 
    assert book.add(10, "sell", 1.0, 5) == []
    assert book.best_ask() == 1.0

def test_market_order_on_empty_book(book):
    assert book.add(1, "sell", None, 10) == []
    assert book.best_ask() is None and len(book) == 0

def test_market_order_ignores_price_limit(book):
    book.add(1, "buy", 50.0, 1)
    book.add(2, "buy", 1.0, 1) 
    assert book.add(9, "sell", None, 2) == [(9, 1, 50.0, 1), (9, 2, 1.0, 1)]

def test_market_order_id_can_be_reused(book):
    book.add(1, "sell", None, 5)
    book.add(1, "buy", 100.0, 5)
    assert book.best_bid() == 100.0



#ensure FIFO for each price
def test_fifo_older_order_fills_first(book):
    book.add(1, "buy", 100.0, 5)  
    book.add(2, "buy", 100.0, 5)
    assert book.add(9, "sell", 100.0, 5) == [(9, 1, 100.0, 5)]
    assert 1 not in book and book.resting_qty(2) == 5

def test_fifo_split_across_same_price_orders(book):
    book.add(1, "sell", 100.0, 4)
    book.add(2, "sell", 100.0, 4)
    book.add(3, "sell", 100.0, 4)
    fills = book.add(9, "buy", 100.0, 6)
    assert fills == [(9, 1, 100.0, 4), (9, 2, 100.0, 2)]
    assert book.depth("sell") == [(100.0, 6)]

def test_partial_fill_keeps_queue_position(book):
    book.add(1, "sell", 100.0, 10)
    book.add(2, "sell", 100.0, 10)
    book.add(8, "buy", 100.0, 3) 
    fills = book.add(9, "buy", 100.0, 8)
    assert fills == [(9, 1, 100.0, 7), (9, 2, 100.0, 1)] 

def test_price_beats_time(book):
    book.add(1, "sell", 101.0, 5)  
    book.add(2, "sell", 100.0, 5)
    assert book.add(9, "buy", 101.0, 5) == [(9, 2, 100.0, 5)]

def test_cancel_and_readd_loses_priority(book):
    book.add(1, "buy", 100.0, 5)
    book.add(2, "buy", 100.0, 5)
    book.cancel(1)
    book.add(1, "buy", 100.0, 5) 
    assert book.add(9, "sell", 100.0, 5) == [(9, 2, 100.0, 5)]

#testing cancelling and lazy deletion
def test_cancel_then_cross_does_not_trade_dead_order(book):
    book.add(1, "sell", 100.0, 5)
    book.add(2, "sell", 100.0, 5)
    assert book.cancel(1) is True
    fills = book.add(9, "buy", 100.0, 10)
    assert fills == [(9, 2, 100.0, 5)]
    assert book.resting_qty(9) == 5 and book.best_bid() == 100.0

def test_cancel_only_order_at_best_level_then_cross(book):
    book.add(1, "sell", 100.0, 5)
    book.add(2, "sell", 101.0, 5)
    book.cancel(1) 
    fills = book.add(9, "buy", 101.0, 5)
    assert fills == [(9, 2, 101.0, 5)]

def test_best_bid_skips_stale_levels(book):
    for i, p in enumerate([100.0, 101.0, 102.0, 103.0]):
        book.add(i, "buy", p, 1)
    book.cancel(3) 
    book.cancel(2)
    assert book.best_bid() == 101.0
    book.cancel(1)
    book.cancel(0)
    assert book.best_bid() is None

def test_best_ask_skips_stale_levels(book):
    book.add(1, "sell", 100.0, 1)
    book.add(2, "sell", 105.0, 1)
    book.cancel(1)
    assert book.best_ask() == 105.0

def test_level_revived_after_going_stale(book):
    book.add(1, "buy", 100.0, 1)
    book.add(2, "buy", 99.0, 1)
    book.cancel(1) 
    book.add(3, "buy", 100.0, 4) 
    assert book.best_bid() == 100.0
    assert book.depth("buy") == [(100.0, 4), (99.0, 1)]
    book._check_invariants() 

def test_cancel_return_values(book):
    book.add(1, "buy", 100.0, 5)
    assert book.cancel(1) is True
    assert book.cancel(1) is False 
    assert book.cancel(42) is False 
    book.add(2, "sell", 100.0, 5)
    book.add(3, "buy", 100.0, 5) 
    assert book.cancel(2) is False 

def test_cancel_partially_filled_order_removes_only_remainder(book):
    book.add(1, "sell", 100.0, 10)
    book.add(2, "sell", 100.0, 1)
    book.add(9, "buy", 100.0, 4)
    assert book.depth("sell") == [(100.0, 7)]
    book.cancel(1)
    assert book.depth("sell") == [(100.0, 1)]


def test_many_cancels_compact_queue_and_heap(book):
    for i in range(200):
        book.add(i, "buy", 100.0, 1)
    for i in range(0, 199):
        book.cancel(i)
    book._check_invariants()
    assert book.add(999, "sell", 100.0, 1) == [(999, 199, 100.0, 1)]
    for i in range(1000, 1500):
        book.add(i, "sell", float(i), 1)
    for i in range(1000, 1499):
        book.cancel(i)
    book._check_invariants()
    assert len(book._heap["sell"]) < 200 
    assert book.best_ask() == 1499.0

#other tests
def test_depth_aggregates_and_orders(book):
    book.add(1, "buy", 99.0, 2)
    book.add(2, "buy", 100.0, 3)
    book.add(3, "buy", 100.0, 4)
    book.add(4, "buy", 98.0, 1)
    book.add(5, "sell", 101.0, 6)
    assert book.depth("buy", 2) == [(100.0, 7), (99.0, 2)]
    assert book.depth("bid") == [(100.0, 7), (99.0, 2), (98.0, 1)]
    assert book.depth("ask", 5) == [(101.0, 6)]
    assert book.depth("buy", 0) == []

def test_empty_book(book):
    assert book.best_bid() is None and book.best_ask() is None
    assert book.depth("buy") == [] and book.depth("sell") == []

@pytest.mark.parametrize("args", [
    (1, "buy", 100.0, 0),
    (1, "buy", 100.0, -5),
    (1, "buy", 100.0, 1.5),
    (1, "hold", 100.0, 1),
    (1, "buy", float("nan"), 1),
])
def test_rejects_bad_input(book, args):
    with pytest.raises(ValueError):
        book.add(*args)

def test_rejects_duplicate_live_id(book):
    book.add(1, "buy", 100.0, 1)
    with pytest.raises(ValueError):
        book.add(1, "sell", 200.0, 1)

#differential test on correct O(N) model
class NaiveBook:
    def __init__(self):
        self.resting = []
        self.seq = 0

    def add(self, oid, side, price, qty):
        opp = "sell" if side == "buy" else "buy"
        fills = []
        while qty:
            cands = [o for o in self.resting if o[1] == opp and (
                price is None or (o[2] <= price if side == "buy" else o[2] >= price))]
            if not cands:
                break
            key = (lambda o: (o[2], o[4])) if opp == "sell" else (lambda o: (-o[2], o[4]))
            m = min(cands, key=key)
            t = min(qty, m[3])
            m[3] -= t
            qty -= t
            fills.append((oid, m[0], m[2], t))
            if m[3] == 0:
                self.resting.remove(m)
        if qty and price is not None:
            self.seq += 1
            self.resting.append([oid, side, price, qty, self.seq])
        return fills

    def cancel(self, oid):
        for o in self.resting:
            if o[0] == oid:
                self.resting.remove(o)
                return True
        return False

    def best(self, side):
        ps = [o[2] for o in self.resting if o[1] == side]
        return (max(ps) if side == "buy" else min(ps)) if ps else None

    def depth(self, side, n):
        agg = {}
        for o in self.resting:
            if o[1] == side:
                agg[o[2]] = agg.get(o[2], 0) + o[3]
        return sorted(agg.items(), key=lambda kv: -kv[0] if side == "buy" else kv[0])[:n]


@pytest.mark.parametrize("seed", range(25))
def test_matches_reference_model(seed):
    rng = random.Random(seed)
    fast, ref = OrderBook(), NaiveBook()
    live, next_id = [], 0
    for step in range(1500):
        r = rng.random()
        if r < 0.35 and live:
            oid = live.pop(rng.randrange(len(live)))
            assert fast.cancel(oid) == ref.cancel(oid)
        else:
            next_id += 1
            side = rng.choice(("buy", "sell"))
            price = None if r > 0.95 else float(rng.randint(95, 105))
            qty = rng.randint(1, 20)
            assert fast.add(next_id, side, price, qty) == ref.add(next_id, side, price, qty), step
            if price is not None:
                live.append(next_id)
        assert fast.best_bid() == ref.best("buy")
        assert fast.best_ask() == ref.best("sell")
        if step % 50 == 0:
            assert fast.depth("buy", 5) == ref.depth("buy", 5)
            assert fast.depth("sell", 5) == ref.depth("sell", 5)
            fast._check_invariants()
    fast._check_invariants()