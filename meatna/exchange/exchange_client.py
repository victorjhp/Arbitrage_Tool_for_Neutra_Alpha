from __future__ import annotations

import asyncio
from typing import Dict, List, Optional

import ccxt.async_support as ccxt

from meatna.core.config_loader import load_secrets
from meatna.exchange.models import Orderbook, OrderbookLevel, MarketInfo


class CcxtUnifiedClient:

    def __init__(
        self,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        exchanges: Optional[List[str]] = None,
    ) -> None:

        self.loop = loop or asyncio.get_event_loop()

        secrets = load_secrets()

        if exchanges is None:
            exchanges = [
                e for e in secrets.keys()
                if e in ("coinbase", "kraken")
            ]

        self.exchanges: Dict[str, ccxt.Exchange] = {}
        self.markets: Dict[str, Dict[str, MarketInfo]] = {}

        for name in exchanges:
            cfg = secrets.get(name, {})
            api_key = cfg.get("api_key", "")
            api_sec = cfg.get("api_secret", "")

            cls = getattr(ccxt, name)
            client = cls({
                "apiKey": api_key,
                "secret": api_sec,
                "enableRateLimit": True,
            })
            self.exchanges[name] = client

    async def load_markets(self) -> None:
        out: Dict[str, Dict[str, MarketInfo]] = {}

        for ex_name, client in self.exchanges.items():
            raw = await client.load_markets()
            mk: Dict[str, MarketInfo] = {}

            for symbol, info in raw.items():
                base = info.get("base")
                quote = info.get("quote")
                if not base or not quote:
                    continue

                market_code = f"{quote}-{base}"

                mk[market_code] = MarketInfo(
                    market=market_code,
                    base_currency=base,
                    quote_currency=quote,
                )

            out[ex_name] = mk

        self.markets = out

    async def fetch_orderbook(self, exchange: str, market_code: str) -> Optional[Orderbook]:
        if exchange not in self.exchanges:
            return None

        if "-" not in market_code:
            return None

        quote, base = market_code.split("-")
        symbol = f"{base}/{quote}"

        ob = await self.exchanges[exchange].fetch_order_book(symbol, limit=25)
        if not ob:
            return None

        bids = [OrderbookLevel(float(p), float(q)) for p, q in ob.get("bids", [])]
        asks = [OrderbookLevel(float(p), float(q)) for p, q in ob.get("asks", [])]

        return Orderbook(
            market=market_code,
            bids=bids,
            asks=asks,
            timestamp=int(ob.get("timestamp") or 0),
        )

    async def fetch_balance(self, exchange: str) -> Dict[str, float]:
        if exchange not in self.exchanges:
            return {}

        bal = await self.exchanges[exchange].fetch_balance()
        out: Dict[str, float] = {}

        for asset in ["USDC", "USD", "BTC", "USDT", "ETH"]:
            info = bal.get(asset)
            if isinstance(info, dict):
                out[asset] = float(info.get("free", 0))
            else:
                out[asset] = 0.0

        return out

    async def close(self) -> None:
        for client in self.exchanges.values():
            try:
                await client.close()
            except:
                pass


class CcxtMultiClient:

    def __init__(
        self,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        exchanges: Optional[List[str]] = None,
    ) -> None:

        self.loop = loop or asyncio.get_event_loop()

        secrets = load_secrets()

        if exchanges is None:
            exchanges = [e for e in secrets.keys() if e in ("coinbase", "kraken")]

        self.exchanges: Dict[str, ccxt.Exchange] = {}
        self.markets: Dict[str, Dict[str, MarketInfo]] = {}

        for name in exchanges:
            cfg = secrets.get(name, {})
            api_key = cfg.get("api_key", "")
            api_secret = cfg.get("api_secret", "")

            cls = getattr(ccxt, name)
            client = cls({
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
            })
            self.exchanges[name] = client

    async def load_markets(self) -> None:
        out = {}

        for ex_name, client in self.exchanges.items():
            raw = await client.load_markets()
            mks = {}

            for symbol, info in raw.items():
                base = info.get("base")
                quote = info.get("quote")
                if not base or not quote:
                    continue

                market_code = f"{quote}-{base}"

                mks[market_code] = MarketInfo(
                    market=market_code,
                    base_currency=base,
                    quote_currency=quote,
                )

            out[ex_name] = mks

        self.markets = out

    async def fetch_orderbook(self, exchange: str, market_code: str) -> Optional[Orderbook]:
        if exchange not in self.exchanges:
            return None

        quote, base = market_code.split("-")
        symbol = f"{base}/{quote}"

        ob = await self.exchanges[exchange].fetch_order_book(symbol, limit=25)
        if not ob:
            return None

        bids = [OrderbookLevel(float(p), float(q)) for p, q in ob.get("bids", [])]
        asks = [OrderbookLevel(float(p), float(q)) for p, q in ob.get("asks", [])]

        return Orderbook(
            market=market_code,
            bids=bids,
            asks=asks,
            timestamp=int(ob.get("timestamp") or 0),
        )

    async def fetch_balance(self, exchange: str) -> Dict[str, float]:
        if exchange not in self.exchanges:
            return {}

        bal = await self.exchanges[exchange].fetch_balance()
        out = {}

        for asset in ["USDC", "USD", "BTC", "USDT", "ETH"]:
            info = bal.get(asset)
            if isinstance(info, dict):
                out[asset] = float(info.get("free", 0))
            else:
                out[asset] = 0.0

        return out

    async def close(self) -> None:
        for client in self.exchanges.values():
            try:
                await client.close()
            except:
                pass
