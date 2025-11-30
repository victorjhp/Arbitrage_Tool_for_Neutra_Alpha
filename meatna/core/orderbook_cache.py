from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

from meatna.utils.logging import debug_log

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrderbookLevel:
    price: float
    size: float


@dataclass(frozen=True)
class OrderbookSnapshot:
    exchange: str
    market: str
    bids: Sequence[OrderbookLevel]
    asks: Sequence[OrderbookLevel]
    timestamp_ms: int


class OrderbookCache:

    def __init__(self, single_exchange: str = None) -> None:
        self._books: Dict[str, OrderbookSnapshot] = {}
        self._lock = asyncio.Lock()
        self._single_exchange = single_exchange

    async def update_snapshot(self, exchange: str, market: str, orderbook: dict) -> None:
        if not orderbook: 
            return
        
        raw_bids = orderbook.get("bids", [])
        raw_asks = orderbook.get("asks", [])
        timestamp = orderbook.get("timestamp", 0)

        bids=[
            OrderbookLevel(float(p), float(q))
            for p, q in raw_bids
            if p > 0 and q > 0
        ]
        asks=[
            OrderbookLevel(float(p), float(q))
            for p, q in raw_asks
            if p > 0 and q > 0
        ]
        if not bids or not asks:
            key = f"{exchange}.{market}"
            debug_log(logger, f"Skipping {key} — no positive bids/asks")
            return
        
        bids.sort(key=lambda x: x.price, reverse=True)
        asks.sort(key=lambda x: x.price)

        snapshot = OrderbookSnapshot(
            exchange=exchange,
            market=market,
            bids=tuple(bids),
            asks=tuple(asks),
            timestamp_ms=int(timestamp or 0),
        )
        await self.update_snapshot(snapshot)
 

    async def update(self, exchange: str, market: str, orderbook: dict) -> None:
        if not orderbook:
            return

        raw_bids = orderbook.get("bids", [])
        raw_asks = orderbook.get("asks", [])
        timestamp = orderbook.get("timestamp", 0)

        bids = [
            OrderbookLevel(float(p), float(q))
            for p, q in raw_bids
            if p > 0 and q > 0
        ]
        asks = [
            OrderbookLevel(float(p), float(q))
            for p, q in raw_asks
            if p > 0 and q > 0
        ]

        if not bids or not asks:
            key = f"{exchange}.{market}"
            debug_log(logger, f"Skipping {key} — no positive bids/asks")
            return

        bids.sort(key=lambda x: x.price, reverse=True)
        asks.sort(key=lambda x: x.price)

        snapshot = OrderbookSnapshot(
            exchange=exchange,
            market=market,
            bids=tuple(bids),
            asks=tuple(asks),
            timestamp_ms=int(timestamp or 0),
        )

        await self.update_snapshot(snapshot)

    async def get_snapshot(self, exchange: str, market: str) -> Optional[OrderbookSnapshot]:
        key = f"{exchange}.{market}"
        async with self._lock:
            return self._books.get(key)

    async def snapshot(self, market: str) -> Optional[OrderbookSnapshot]:
        async with self._lock:
            if self._single_exchange:
                if "::" in market:
                    market = market.split("::", 1)[1]
                return self._books.get(market)
            else:
                for _, snap in self._books.items():
                    if snap.market == market:
                        return snap
                return None

    async def snapshot_many(
        self,
        exchange: str,
        markets: Iterable[str],
    ) -> Mapping[str, OrderbookSnapshot]:
        async with self._lock:
            return {
                m: self._books[f"{exchange}.{m}"]
                for m in markets
                if f"{exchange}.{m}" in self._books
            }

    async def markets(self) -> List[str]:
        async with self._lock:
            return list(self._books.keys())

    async def has_data(self) -> bool:
        async with self._lock:
            return bool(self._books)
