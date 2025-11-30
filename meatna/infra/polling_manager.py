from __future__ import annotations

import asyncio
import logging
from typing import Sequence

import ccxt.async_support as ccxt

from meatna.exchange.models import OrderbookLevel, Ticker
from meatna.core.orderbook_cache import OrderbookCache
from meatna.core.volatility_cache import VolatilityCache

logger = logging.getLogger(__name__)


class WSManager:

    def __init__(
        self,
        orderbook_cache: OrderbookCache,
        vol_cache: VolatilityCache,
        orderbook_markets: Sequence[str],
        ticker_markets: Sequence[str],
        exchange_name: str = "coinbase",
        debug: bool = False,
        poll_interval_sec: float = 0.4,
    ) -> None:
        self._orderbook_cache = orderbook_cache
        self._vol_cache = vol_cache
        self._orderbook_markets = list(orderbook_markets)
        self._ticker_markets = list(ticker_markets)
        self._debug = debug
        self._poll_interval = poll_interval_sec

        self._tasks: list[asyncio.Task] = []
        self._exchange_name = exchange_name
        self._exchange = getattr(ccxt, exchange_name)({"enableRateLimit": True})

    async def start(self) -> None:
        await self._exchange.load_markets()

        self._tasks.append(asyncio.create_task(self._poll_orderbooks()))
        self._tasks.append(asyncio.create_task(self._poll_tickers()))

        if self._debug:
            logger.info(
                "WSManager(%s) started: %d orderbooks, %d tickers",
                self._exchange.id,
                len(self._orderbook_markets),
                len(self._ticker_markets),
            )

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await self._exchange.close()

    async def _poll_orderbooks(self) -> None:
        try:
            while True:
                for market in self._orderbook_markets:
                    try:
                        quote, base = market.split("-")
                        symbol = f"{base}/{quote}"

                        ob = await self._exchange.fetch_order_book(symbol, limit=15)
                        if not ob:
                            continue

                        await self._orderbook_cache.update(
                            self._exchange_name,
                            market,
                            ob,
                        )

                    except Exception as exc:
                        if self._debug:
                            logger.warning("[WS-OB] Failed for %s (%s): %s", self._exchange_name, market, exc)
                        continue

                await asyncio.sleep(self._poll_interval)

        except asyncio.CancelledError:
            return

    async def _poll_tickers(self) -> None:
        try:
            while True:
                for market in self._ticker_markets:
                    try:
                        quote, base = market.split("-")
                        symbol = f"{base}/{quote}"

                        t = await self._exchange.fetch_ticker(symbol)
                        if "last" not in t:
                            continue

                        ticker = Ticker(
                            market=market,
                            timestamp=int(t.get("timestamp") or 0),
                            trade_price=float(t["last"]),
                        )
                        await self._vol_cache.update_from_ticker(ticker)

                    except Exception as exc:
                        if self._debug:
                            logger.warning("[WS-TICK] Failed for %s (%s): %s", self._exchange_name, market, exc)
                        continue

                await asyncio.sleep(self._poll_interval)

        except asyncio.CancelledError:
            return
