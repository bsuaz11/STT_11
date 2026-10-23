from typing import Annotated, Literal, Union
from pydantic import BaseModel, Field, field_validator, StringConstraints

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
PositiveFloat = Annotated[float, Field(gt=0)]
Level = tuple[PositiveFloat, PositiveFloat]   # (price, qty)

class CancelOrder(BaseModel):
    type: Literal["cancel_order"] = "cancel_order"
    team_id: NonEmptyStr
    order_id: NonEmptyStr
    symbol: NonEmptyStr

class Handshake(BaseModel):
    type: Literal["handshake"] = "handshake"
    team_id: NonEmptyStr
    role: Literal["broker", "trader", "observer", "teacher"]
    level: int = Field(ge=0)
    token: str = ""

class PlaceOrder(BaseModel):
    type: Literal["place_order"] = "place_order"
    team_id: NonEmptyStr
    symbol: NonEmptyStr
    side: Literal["buy", "sell"]
    order_type: Literal["limit", "market", "ioc"] = "limit"
    price: float | None = Field(default=None, gt=0)   # None => market order
    quantity: int = Field(gt=0)

class OrderAck(BaseModel):
    type: Literal["order_ack"] = "order_ack"
    order_id: NonEmptyStr
    team_id: NonEmptyStr
    symbol: NonEmptyStr
    side: Literal["buy", "sell"]
    price: float = Field(ge=0) #0.0 means no limit price (market/MOC acks)
    quantity: int = Field(gt=0)
    # Queue standing at the moment the order rests (0 for fully-filled/rejected).
    # queue_ahead = shares that will fill before this order at its price level;
    # level_qty = total shares resting at that price+side. Defaults keep older
    # clients and non-queue builds working unchanged.
    queue_ahead: int = Field(default=0, ge=0)
    level_qty: int = Field(default=0, ge=0)

class BookSnapshot(BaseModel):
    type: Literal["book_snapshot"] = "book_snapshot"
    symbol: NonEmptyStr
    bids: list[Level]   # best (highest) first; [[price, qty], ...]
    asks: list[Level]   # best (lowest) first [[price, qty], ...]
    mid_price: float = Field(ge=0)
    spread: float = Field(ge=0)
    # The venue's own mark: fair value (microprice blended with the shared
    # fundamental) plus undecayed trade impact. Unlike mid_price it exists
    # even when the book is one-sided, and it carries the fundamental, so it
    # is what a market maker should quote AROUND — quoting around the book mid
    # means quoting around your own quotes. Defaults to 0.0 for older venues.
    ref_price: float = Field(default=0.0, ge=0)
    # "equity" | "future" | … — lets a market maker discover NEW listings
    # (IPOs) without hardcoding a symbol list, while skipping futures.
    asset_type: str = "equity"
    # Signal inputs (M6) for alpha-in-a-budget bots. microprice = depth-weighted
    # touch (None→0.0 one-sided); obi = order-book imbalance in [-1, 1] (+bids
    # heavy). Cheap edges you can act on — but computing more costs latency,
    # which costs queue position. Defaults keep older consumers working.
    microprice: float = Field(default=0.0, ge=0)
    obi: float = Field(default=0.0, ge=-1, le=1)

Message = Annotated[
    Union[Handshake, PlaceOrder, CancelOrder, OrderAck, BookSnapshot],
    Field(discriminator="type"),
]
