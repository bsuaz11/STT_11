"""
team/venues.py — multi-venue trading for the trader seats (week 8: NBBO routing, fee wars).

Every venue lists the same stocks but keeps its own book, its own marks, its
own portfolio for our seat and its own fee schedule. This module gives the
trader:

    VenueState        what we know about one venue: touch per symbol, mark,
                      fees, halts, our cash and positions there, freshness
    route()           pick the venue for one IOC: the best price AFTER the
                      taker fee, among venues that are live, coherent and can
                      carry the order (cash; a reduce goes where the position is)
    CombinedPortfolio the trader's view of its positions across venues, so the
                      risk layer and the strategies see one book
    VenueLink         a light client for one extra venue: connects with the
                      team token, keeps its VenueState, forwards our fills
    discover_venues() the venue list (GET <arena>/api/venues) mapped to URLs

Routing rules, and why:
  * Effective price = touch × (1 ± taker fee). A venue charging 200 bps is
    only used when its price is better by more than the fee difference.
  * A venue whose mark is more than COHERENCE_BPS away from the primary's is
    not routable: its book is stale or dislocated, and a position bought
    there is marked there (cheap-looking quotes are not free money).
  * A reducing order goes to a venue that holds the position, capped at what
    is held there: each venue has its own portfolio, so selling on venue B
    what was bought on venue A would leave a long on A and a short on B.
  * Prices are re-derived from the chosen venue's touch and clamped inside
    ITS LULD band (each venue checks the band against its own mark).
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

COHERENCE_BPS = 100.0        # a venue's mark this far from the primary's is not routable
STALE_SEC = 5.0              # no book from a venue for this long: not routable
ROUTE_SLIP_BPS = 20.0        # how far through the chosen venue's touch an IOC may reach
CASH_USE = 0.90              # spend at most this share of a venue's cash on one buy
DEFAULT_TAKER = 0.0015       # until a venue publishes its schedule (FEE_SCHEDULE)
MAX_SECONDARY = 4            # extra venues the trader connects to at most


@dataclass
class Touch:
    """Best bid/ask with sizes and the venue mark, for one symbol on one venue."""
    bid: float | None = None
    bid_qty: float = 0.0
    ask: float | None = None
    ask_qty: float = 0.0
    ref: float = 0.0
    at: float = 0.0              # monotonic time of the snapshot


@dataclass
class VenueState:
    """Everything the router needs to know about one venue."""
    name: str
    primary: bool = False
    taker: float = DEFAULT_TAKER
    rebate: float = 0.0
    quotes: dict[str, Touch] = field(default_factory=dict)
    halted: set[str] = field(default_factory=set)
    market_halted: bool = False
    in_auction: bool = False
    session_open: bool = True
    connected: bool = False
    cash: float | None = None
    positions: dict[str, int] = field(default_factory=dict)
    base_worth: float | None = None   # net worth when first seen: what it funded us with
    luld: float = 0.08
    orders: int = 0                   # orders sent here (for the log and the tests)

    def on_book(self, symbol: str, bids: list, asks: list, ref: float,
                now: float | None = None) -> None:
        """Store the touch from a BookSnapshot (lists of [price, qty])."""
        t = self.quotes.setdefault(symbol, Touch())
        t.bid, t.bid_qty = (float(bids[0][0]), float(bids[0][1])) if bids else (None, 0.0)
        t.ask, t.ask_qty = (float(asks[0][0]), float(asks[0][1])) if asks else (None, 0.0)
        if ref and ref > 0:
            t.ref = float(ref)
        t.at = time.monotonic() if now is None else now

    def on_fee_schedule(self, data: dict) -> None:
        """FEE_SCHEDULE data: taker and rebate as fractions of notional."""
        if isinstance(data.get("taker"), (int, float)):
            self.taker = float(data["taker"])
        if isinstance(data.get("rebate"), (int, float)):
            self.rebate = float(data["rebate"])

    def on_halt_event(self, event: str, data: dict) -> None:
        """Track this venue's own halts (its circuit breakers run on its marks)."""
        sym = data.get("symbol")
        if event == "TRADING_HALT" and sym:
            self.halted.add(sym)
        elif event == "TRADING_RESUMED" and sym:
            self.halted.discard(sym)
        elif event == "MARKET_HALT":
            self.market_halted = True
        elif event == "MARKET_RESUMED":
            self.market_halted = False

    def usable(self, symbol: str, now: float | None = None) -> bool:
        """Live, fresh, open and not halted in `symbol`."""
        now = time.monotonic() if now is None else now
        t = self.quotes.get(symbol)
        return (self.connected and self.session_open and not self.in_auction
                and not self.market_halted and symbol not in self.halted
                and t is not None and now - t.at <= STALE_SEC)

    def worth(self, marks: dict[str, float]) -> float | None:
        """Cash + positions at `marks` (None until the venue has told us our cash)."""
        if self.cash is None:
            return None
        return self.cash + sum(q * marks.get(s, 0.0) for s, q in self.positions.items() if q)


@dataclass(frozen=True)
class Route:
    """One routed IOC."""
    venue: str
    quantity: int
    price: float
    effective: float            # touch after the taker fee
    why: str


def _band(ref: float, luld: float, px: float) -> float:
    """Clamp px inside the venue's LULD band around its own mark (0.5% margin)."""
    if not ref or luld <= 0:
        return px
    band = max(0.0, luld - 0.005)
    lo, hi = math.ceil(ref * (1 - band) * 100) / 100, math.floor(ref * (1 + band) * 100) / 100
    return min(max(px, lo), hi)


def route(symbol: str, side: str, quantity: int, limit: float, venues: list[VenueState],
          *, now: float | None = None, coherence_bps: float = COHERENCE_BPS,
          slip_bps: float = ROUTE_SLIP_BPS) -> Route | None:
    """The venue for one IOC, or None if no venue can take it.

    `limit` is the price the strategy is willing to pay (buy) or accept
    (sell), already gated. The order is reducing on a venue if that venue
    holds the opposite side of `side`; reduces are preferred (and capped at
    the position there), so a close never opens a new position elsewhere.
    """
    if quantity <= 0:
        return None
    primary = next((v for v in venues if v.primary), None)
    anchor = primary.quotes.get(symbol).ref if primary and primary.quotes.get(symbol) else 0.0
    sign = 1 if side == "buy" else -1
    total = sum(v.positions.get(symbol, 0) for v in venues)
    reducing = total * sign < 0
    best: Route | None = None
    for v in venues:
        if not v.usable(symbol, now):
            continue
        t = v.quotes[symbol]
        touch = t.ask if side == "buy" else t.bid
        if not touch or touch <= 0:
            continue
        if side == "buy" and touch > limit or side == "sell" and touch < limit:
            continue                                   # the IOC would not fill here
        ref = t.ref or touch
        if anchor and not v.primary and abs(ref / anchor - 1) * 1e4 > coherence_bps:
            continue                                   # dislocated venue
        held = v.positions.get(symbol, 0)
        qty = quantity
        if reducing:
            if held * sign >= 0:
                continue                               # nothing to reduce here
            qty = min(qty, abs(held))
        elif side == "buy" and v.cash is not None:
            qty = min(qty, int(max(0.0, v.cash) * CASH_USE / touch))
        if qty <= 0:
            continue
        eff = touch * (1 + sign * v.taker)
        px = touch * (1 + sign * slip_bps / 1e4)
        px = min(px, limit) if side == "buy" else max(px, limit)
        px = _band(ref, v.luld, round(px, 2))
        cand = Route(v.name, qty, px, eff, f"{'reduce' if reducing else 'open'} "
                     f"{side} {symbol}: {v.name} {touch:.2f} + {v.taker * 1e4:.0f} bps = {eff:.4f}")
        better = (best is None or (eff < best.effective if side == "buy" else eff > best.effective)
                  or (eff == best.effective and qty > best.quantity))
        if better:
            best = cand
    return best


class CombinedPortfolio:
    """The trader's portfolio across venues: primary + every extra venue.

    positions   summed per symbol (what the risk layer and strategies see)
    cash        spendable cash summed (route() re-checks it per venue)
    capital_offset  starting capital of the extra venues, so equity used for
                the session stop only counts their P&L, not the cash they
                fund (a venue connecting mid-session is not a gain, and one
                dropping out is not a loss)
    """

    def __init__(self, primary: Any, extra: list[VenueState]) -> None:
        self.primary = primary
        self.extra = extra

    @property
    def positions(self) -> dict[str, int]:
        out = dict(self.primary.positions)
        for v in self.extra:
            for s, q in v.positions.items():
                if q:
                    out[s] = out.get(s, 0) + q
        return {s: q for s, q in out.items() if q}

    @positions.setter
    def positions(self, value: dict[str, int]) -> None:
        self.primary.positions = value

    @property
    def cash(self) -> float:
        return self.primary.cash + sum(v.cash or 0.0 for v in self.extra)

    @cash.setter
    def cash(self, value: float) -> None:
        self.primary.cash = value

    @property
    def capital_offset(self) -> float:
        return sum(v.base_worth or 0.0 for v in self.extra if v.cash is not None)

    def can_buy(self, symbol: str, qty: int, price: float) -> bool:
        return self.cash >= price * qty * 1.001

    def can_sell(self, symbol: str, qty: int) -> bool:
        return self.positions.get(symbol, 0) >= qty

    def __getattr__(self, name: str) -> Any:   # realized_pnl, total_fees_paid, …
        return getattr(self.primary, name)


def venue_url(primary_url: str, port: int) -> str:
    """The websocket URL of the venue on `port`, next to the primary.

    Hosted (TLS): the primary is wss://feed.<domain> and venue P is
    wss://feed-P.<domain>. Local: ws://host:P.
    """
    u = urlparse(primary_url)
    host = u.hostname or "localhost"
    if u.scheme == "wss" and host.startswith("feed."):
        return f"wss://feed-{port}.{host[len('feed.'):]}"
    return f"{u.scheme or 'ws'}://{host}:{port}"


def discover_venues(primary_url: str, http_base: str | None = None,
                    timeout: float = 5.0) -> list[dict]:
    """Non-primary venues from GET <arena>/api/venues, cheapest taker first.

    Each item: {"name", "port", "url", "taker_bps", "rebate_bps"}. Returns []
    if the arena cannot be reached (the trader then runs single-venue).
    """
    u = urlparse(primary_url)
    host = u.hostname or "localhost"
    if http_base is None:
        if u.scheme == "wss" and host.startswith("feed."):
            http_base = f"https://{host[len('feed.'):]}"
        else:
            http_base = f"http://{host}:{u.port or 8765}"
    try:
        with urllib.request.urlopen(f"{http_base}/api/venues", timeout=timeout) as r:
            data = json.load(r)
    except Exception as exc:  # noqa: BLE001 — no venue list: the trader runs single-venue
        logger.warning("Venue list unavailable (%s): trading the primary venue only", exc)
        return []
    out = []
    for v in data.get("venues", []):
        if v.get("team") is None:           # the primary (house) venue
            continue
        port = int(v["port"])
        out.append({"name": str(v.get("venue") or port), "port": port,
                    "url": venue_url(primary_url, port),
                    "taker_bps": float(v.get("taker_bps", 0)),
                    "rebate_bps": float(v.get("rebate_bps", 0))})
    out.sort(key=lambda v: v["taker_bps"])
    return out


class VenueLink:
    """A light client for one extra venue.

    Keeps `state` (touch, fees, halts, our cash and positions there) up to
    date, forwards OUR fills to `on_fill(side, symbol, qty, price, venue)` and
    venue rejects to `on_reject(code, message, venue)`, and sends orders.
    Reconnects on its own; while it is down the router simply skips it.
    Session news (calendar, IPOs, shocks) comes from the primary only.
    """

    def __init__(self, name: str, url: str, team_id: str, token: str,
                 on_fill: Callable[[str, str, int, float, str], None],
                 on_reject: Callable[[str | None, str | None, str], None]) -> None:
        self.state = VenueState(name=name)
        self.url = url
        self.team_id = team_id
        self.token = token
        self._on_fill = on_fill
        self._on_reject = on_reject
        self._ws: Any = None
        self._synced = False

    async def send_order(self, symbol: str, side: str, quantity: int, price: float,
                         order_type: str = "ioc") -> bool:
        """Send one order; False if the link is down."""
        from shared.messages import PlaceOrder
        if self._ws is None or not self.state.connected:
            return False
        try:
            await self._ws.send(PlaceOrder(team_id=self.team_id, symbol=symbol, side=side,
                                           order_type=order_type, price=price,
                                           quantity=quantity).model_dump_json())
        except Exception:  # noqa: BLE001 — any send failure means the link is down
            logger.warning("Venue %s: send failed — marking it down", self.state.name)
            self.state.connected = False
            return False
        self.state.orders += 1
        return True

    def handle(self, raw: str) -> None:
        """Apply one message from the venue (public for the tests)."""
        msg = json.loads(raw)
        kind = msg.get("type")
        st = self.state
        if kind == "book_snapshot":
            st.on_book(msg["symbol"], msg.get("bids") or [], msg.get("asks") or [],
                       float(msg.get("ref_price") or 0.0))
        elif kind == "portfolio_update" and msg.get("team_id") == self.team_id:
            st.cash = float(msg["cash"])
            st.positions = {s: int(q) for s, q in (msg.get("positions") or {}).items() if q}
            if st.base_worth is None:
                st.base_worth = float(msg.get("net_worth", st.cash))
            self._synced = True
        elif kind == "trade_execution":
            if msg.get("buyer_id") == self.team_id:
                self._on_fill("buy", msg["symbol"], int(msg["quantity"]), float(msg["price"]), st.name)
            elif msg.get("seller_id") == self.team_id:
                self._on_fill("sell", msg["symbol"], int(msg["quantity"]), float(msg["price"]), st.name)
        elif kind == "leaderboard" and not self._synced:
            me = next((e for e in msg.get("traders", []) if e.get("team_id") == self.team_id), None)
            if me is not None and "cash" in me:
                st.cash = float(me["cash"])
                st.positions = {s: int(q) for s, q in (me.get("positions") or {}).items() if q}
                if st.base_worth is None:
                    st.base_worth = float(me.get("net_worth", st.cash))
                self._synced = True
        elif kind == "session_event":
            ev, data = msg.get("event", ""), msg.get("data") or {}
            if ev == "FEE_SCHEDULE":
                st.on_fee_schedule(data)
                logger.info("Venue %s fees: taker %.1f bps, rebate %.1f bps",
                            st.name, st.taker * 1e4, st.rebate * 1e4)
            elif ev in ("SESSION_OPEN", "SESSION_PREOPEN"):
                st.session_open = True
                st.in_auction = ev == "SESSION_PREOPEN"
                cfg = ((data.get("scenario") or {}).get("config") or {})
                st.luld = float(cfg.get("LULD_BAND_PCT", st.luld))
            elif ev == "SESSION_PRECLOSE":
                st.in_auction = True
            elif ev == "SESSION_CLOSED":
                st.session_open = False
            st.on_halt_event(ev, data)
        elif kind == "error":
            code = msg.get("code")
            if code == "SYMBOL_HALTED":            # this venue's halt, not the primary's
                sym = next((s for s in st.quotes if s in (msg.get("message") or "")), None)
                if sym:
                    st.halted.add(sym)
            self._on_reject(code, msg.get("message"), st.name)

    async def run(self) -> None:
        """Connect, listen, reconnect — forever."""
        import websockets
        from shared.messages import Handshake
        delay = 3.0
        while True:
            try:
                async with websockets.connect(self.url) as ws:
                    await ws.send(Handshake(team_id=self.team_id, role="trader", level=1,
                                            token=self.token).model_dump_json())
                    self._ws, self._synced = ws, False
                    self.state.connected = True
                    logger.info("Venue %s connected (%s)", self.state.name, self.url)
                    async for raw in ws:
                        try:
                            self.handle(raw)
                        except Exception: 
                            logger.exception("Venue %s: unreadable message ignored", self.state.name)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — a venue failing must not stop the trader
                logger.warning("Venue %s down (%s) — retrying in %.0fs", self.state.name, exc, delay)
            finally:
                self.state.connected = False
                self._ws = None
            await asyncio.sleep(delay)
