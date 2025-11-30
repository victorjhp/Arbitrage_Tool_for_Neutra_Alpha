from __future__ import annotations

from typing import List, Sequence

import ccxt.async_support as ccxt

from meatna.exchange.models import MarketInfo, OrderbookLevel, Ticker
from meatna.core.orderbook_cache import OrderbookSnapshot


class RestBootstrapper:

    def __init__(
        self,
        api_key: str | None = None,
        secret: str | None = None,
        exchange_name: str = "coinbase",
    ):
        self._exchange_name = exchange_name
        params = {"enableRateLimit": True}

        if api_key and secret:
            params["apiKey"] = api_key
            params["secret"] = secret

        self._exchange = getattr(ccxt, exchange_name)(params)
        self._closed = False

    async def __aenter__(self) -> "RestBootstrapper":
        await self._exchange.load_markets()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        if not self._closed:
            await self._exchange.close()
            self._closed = True

    async def fetch_markets(self) -> List[MarketInfo]:
        markets = await self._exchange.load_markets()
        result: List[MarketInfo] = []

        for symbol, m in markets.items():
            base = m.get("base")
            quote = m.get("quote")
            if not base or not quote:
                continue

            market_code = f"{quote}-{base}"

            result.append(
                MarketInfo(
                    market=market_code,
                    base_currency=base,
                    quote_currency=quote,
                )
            )

        return result

    async def fetch_orderbooks(
        self,
        markets: Sequence[str],
        depth: int = 20,
    ) -> List[OrderbookSnapshot]:

        result: List[OrderbookSnapshot] = []

        for market in markets:
            try:
                quote, base = market.split("-")
                symbol = f"{base}/{quote}"

                ob = await self._exchange.fetch_order_book(symbol, limit=depth)

                bids = [
                    OrderbookLevel(price=float(p), size=float(q))
                    for p, q in ob.get("bids", [])
                    if p > 0 and q > 0
                ]
                asks = [
                    OrderbookLevel(price=float(p), size=float(q))
                    for p, q in ob.get("asks", [])
                    if p > 0 and q > 0
                ]

                if not bids or not asks:
                    continue

                snapshot = OrderbookSnapshot(
                    exchange=self._exchange_name,
                    market=market,
                    bids=bids,
                    asks=asks,
                    timestamp_ms=int(ob.get("timestamp") or 0),
                )

                result.append(snapshot)

            except Exception:
                continue

        return result

    async def fetch_ticker(self, market: str) -> Ticker:
        quote, base = market.split("-")
        symbol = f"{base}/{quote}"

        t = await self._exchange.fetch_ticker(symbol)

        return Ticker(
            market=market,
            timestamp=int(t.get("timestamp") or 0),
            trade_price=float(t["last"]),
        )
