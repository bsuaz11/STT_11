from __future__ import annotations
import heapq
import math
from collections import deque

BUY = "buy"
SELL = "sell"

_SIDES = {
    "buy": BUY, "bid": BUY, "b": BUY,
    "sell": SELL, "ask": SELL, "s": SELL, "offer": SELL,
}


_QUEUE_COMPACT_MIN_DEAD = 16
_HEAP_COMPACT_SLACK = 64


def _norm_side(side: str) -> str:
    try:
        return _SIDES[side.lower()]
    except (KeyError, AttributeError):
        raise ValueError(f"unknown side {side!r}") from None


class _Order:
    __slots__ = ("order_id", "side", "price", "qty")

    def __init__(self, order_id: int, side: str, price: float, qty: int):
        self.order_id = order_id
        self.side = side
        self.price = price
        self.qty = qty           


class _Level:
    __slots__ = ("price", "queue", "qty", "live", "dead")

    def __init__(self, price: float):
        self.price = price
        self.queue: deque[_Order] = deque()
        self.qty = 0            
        self.live = 0  
        self.dead = 0    


class OrderBook:
    def __init__(self) -> None:
        self._levels: dict[str, dict[float, _Level]] = {BUY: {}, SELL: {}}
        self._heap: dict[str, list[float]] = {BUY: [], SELL: []}
        self._in_heap: dict[str, set[float]] = {BUY: set(), SELL: set()}
        self._orders: dict[int, _Order] = {}

    def add(self, order_id: int, side: str, price: float | None, qty: int) -> list[tuple]:
        side = _norm_side(side)
        if not isinstance(qty, int) or isinstance(qty, bool) or qty <= 0:
            raise ValueError(f"Quanity needs to be a positive integer")
        if order_id in self._orders:
            raise ValueError(f"order_id is already resting")
        is_market = price is None
        if not is_market:
            price = float(price)
            if not math.isfinite(price):
                raise ValueError(f"Price is not finite")

        fills: list[tuple] = []
        remaining = qty
        opp = SELL if side == BUY else BUY
        opp_levels = self._levels[opp]
        orders = self._orders
        while remaining:
            level = self._best_level(opp)           
            if level is None:
                break
            if not is_market:            
                if side == BUY and level.price > price:
                    break
                if side == SELL and level.price < price:
                    break
            q = level.queue
            while remaining and q:       
                maker = q[0]
                if maker.qty == 0:             
                    level.dead -= 1
                    continue
                traded = remaining if remaining < maker.qty else maker.qty
                maker.qty -= traded
                level.qty -= traded
                remaining -= traded
                fills.append((order_id, maker.order_id, level.price, traded))
                if maker.qty == 0:    
                    q.popleft()
                    level.live -= 1
                    del orders[maker.order_id]
            if level.live == 0:             
                del opp_levels[level.price]
                self._maybe_compact_heap(opp)

        if remaining and not is_market:
            self._rest(_Order(order_id, side, price, remaining))
        return fills

    def cancel(self, order_id: int) -> bool:
        order = self._orders.pop(order_id, None)
        if order is None:
            return False 
        levels = self._levels[order.side]
        level = levels[order.price]
        level.qty -= order.qty
        level.live -= 1
        order.qty = 0       
        if level.live == 0:
            del levels[order.price]         
            self._maybe_compact_heap(order.side)
        else:
            level.dead += 1
            if level.dead > level.live and level.dead >= _QUEUE_COMPACT_MIN_DEAD:
                level.queue = deque(o for o in level.queue if o.qty)
                level.dead = 0
        return True
    def best_bid(self) -> float | None:
        level = self._best_level(BUY)
        return None if level is None else level.price
    def best_ask(self) -> float | None:
        level = self._best_level(SELL)
        return None if level is None else level.price
    def depth(self, side: str, n: int = 5) -> list[tuple[float, int]]:
        side = _norm_side(side)
        if n <= 0:
            return []
        levels = self._levels[side]
        pick = heapq.nlargest if side == BUY else heapq.nsmallest
        return [(p, levels[p].qty) for p in pick(n, levels)]
    def __len__(self) -> int:
        return len(self._orders)
    def __contains__(self, order_id: int) -> bool:
        return order_id in self._orders
    def resting_qty(self, order_id: int) -> int:
        o = self._orders.get(order_id)
        return 0 if o is None else o.qty

    def _rest(self, order: _Order) -> None:
        side, price = order.side, order.price
        levels = self._levels[side]
        level = levels.get(price)
        if level is None:
            level = levels[price] = _Level(price)
            if price not in self._in_heap[side]:    
                self._in_heap[side].add(price)
                heapq.heappush(self._heap[side], -price if side == BUY else price)
        level.queue.append(order)
        level.live += 1
        level.qty += order.qty
        self._orders[order.order_id] = order

    def _best_level(self, side: str) -> _Level | None:
        heap, levels = self._heap[side], self._levels[side]
        neg = side == BUY
        while heap:
            price = -heap[0] if neg else heap[0]
            level = levels.get(price)
            if level is not None:
                return level
            heapq.heappop(heap)
            self._in_heap[side].discard(price)
        return None

    def _maybe_compact_heap(self, side: str) -> None:
        heap, levels = self._heap[side], self._levels[side]
        if len(heap) > 2 * len(levels) + _HEAP_COMPACT_SLACK:
            neg = side == BUY
            new = [-p if neg else p for p in levels]
            heapq.heapify(new)
            self._heap[side] = new
            self._in_heap[side] = set(levels)

    def _check_invariants(self) -> None:
        for side in (BUY, SELL):
            levels = self._levels[side]
            heap_prices = [-k if side == BUY else k for k in self._heap[side]]
            assert len(heap_prices) == len(set(heap_prices)), "duplicate heap entry"
            assert set(heap_prices) == self._in_heap[side]
            for p, lvl in levels.items():
                assert p in self._in_heap[side], "live level missing from heap"
                live = [o for o in lvl.queue if o.qty]
                assert len(live) == lvl.live > 0
                assert sum(o.qty for o in live) == lvl.qty
                assert len(lvl.queue) - len(live) == lvl.dead
                for o in live:
                    assert self._orders[o.order_id] is o
        n_live = sum(l.live for s in (BUY, SELL) for l in self._levels[s].values())
        assert n_live == len(self._orders)