from __future__ import annotations
from dataclasses import dataclass
from typing import List


@dataclass
class Ticker:
    market: str
    timestamp: int
    trade_price: float


@dataclass
class MarketInfo:
    market: str
    base_currency: str
    quote_currency: str


@dataclass
class OrderbookLevel:
    price: float
    size: float


@dataclass
class Orderbook:
    market: str
    bids: List[OrderbookLevel]
    asks: List[OrderbookLevel]
    timestamp: int
