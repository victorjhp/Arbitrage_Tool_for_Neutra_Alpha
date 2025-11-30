from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, Set
from datetime import datetime

try:
    import ccxt.pro as ccxtpro
    CCXT_PRO_AVAILABLE = True
except ImportError:
    CCXT_PRO_AVAILABLE = False
    ccxtpro = None

from meatna.core.config_loader import load_secrets
from meatna.core.orderbook_cache import OrderbookCache, OrderbookSnapshot
from meatna.exchange.models import OrderbookLevel

logger = logging.getLogger(__name__)


class WebSocketManager:

    def __init__(
        self,
        caches: Dict[str, OrderbookCache],
        exchanges: Optional[List[str]] = None
    ):
        if not CCXT_PRO_AVAILABLE:
            raise ImportError(
                "CCXT Pro is required for WebSocket support. "
                "Install with: pip install ccxt[pro]"
            )

        self.caches = caches
        self.exchanges: Dict[str, ccxtpro.Exchange] = {}
        self._tasks: List[asyncio.Task] = []
        self._running = False

        secrets = load_secrets()

        if exchanges is None:
            exchanges = ["coinbase", "kraken"]

        for name in exchanges:
            if name not in caches:
                continue

            cfg = secrets.get(name, {})
            api_key = cfg.get("api_key", "")
            api_secret = cfg.get("api_secret", "")

            try:
                cls = getattr(ccxtpro, name)
                client = cls({
                    "apiKey": api_key,
                    "secret": api_secret,
                    "enableRateLimit": True,
                    "newUpdates": True,
                })
                self.exchanges[name] = client
                logger.info(f"✅ WebSocket client created: {name}")
            except Exception as e:
                logger.error(f"❌ Failed to create WebSocket client for {name}: {e}")

    async def subscribe_orderbooks(
        self,
        exchange: str,
        markets: List[str],
        stop_event: asyncio.Event
    ) -> None:
        if exchange not in self.exchanges:
            logger.warning(f"Exchange {exchange} not available")
            return

        client = self.exchanges[exchange]
        cache = self.caches[exchange]

        logger.info(f"📡 Subscribing to {len(markets)} markets on {exchange}")

        tasks = []
        for market in markets:
            task = asyncio.create_task(
                self._watch_orderbook(client, cache, exchange, market, stop_event)
            )
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"🛑 Stopped watching {len(markets)} markets on {exchange}")

    async def _watch_orderbook(
        self,
        client: ccxtpro.Exchange,
        cache: OrderbookCache,
        exchange: str,
        symbol: str,
        stop_event: asyncio.Event
    ) -> None:
        try:
            base, quote = symbol.split("/")
            market_code = f"{quote}-{base}"
        except:
            logger.error(f"Invalid symbol format: {symbol}")
            return

        update_count = 0
        error_count = 0
        max_errors = 10

        while not stop_event.is_set() and error_count < max_errors:
            try:
                orderbook = await client.watch_order_book(symbol, limit=25)

                if not orderbook or not orderbook.get('bids') or not orderbook.get('asks'):
                    await asyncio.sleep(0.1)
                    continue

                bids = tuple([
                    OrderbookLevel(float(p), float(q))
                    for p, q in orderbook['bids']
                ])
                asks = tuple([
                    OrderbookLevel(float(p), float(q))
                    for p, q in orderbook['asks']
                ])

                snapshot = OrderbookSnapshot(
                    exchange=exchange,
                    market=market_code,
                    bids=bids,
                    asks=asks,
                    timestamp_ms=int(orderbook.get('timestamp', 0)),
                )

                await cache.update_snapshot(snapshot)
                update_count += 1

                if update_count <= 3:
                    logger.info(
                        f"  ✓ {exchange}::{market_code} updated "
                        f"(bid={bids[0].price:.2f}, ask={asks[0].price:.2f})"
                    )

                error_count = 0

            except asyncio.CancelledError:
                logger.debug(f"Watch cancelled for {exchange}::{market_code}")
                break
            except Exception as e:
                error_count += 1
                logger.debug(
                    f"Error watching {exchange}::{market_code} "
                    f"(error {error_count}/{max_errors}): {e}"
                )
                await asyncio.sleep(1.0)

        if error_count >= max_errors:
            logger.error(
                f"❌ Too many errors for {exchange}::{market_code}, stopped watching"
            )

    async def start(
        self,
        markets_needed: Set[str],
        stop_event: asyncio.Event
    ) -> None:
        self._running = True
        logger.info("🚀 Starting WebSocket subscriptions...")

        exchange_markets: Dict[str, List[str]] = {}

        for market_full in markets_needed:
            if "::" not in market_full:
                continue

            exchange, market_code = market_full.split("::", 1)

            if exchange not in self.exchanges:
                continue

            try:
                quote, base = market_code.split("-")
                symbol = f"{base}/{quote}"

                if exchange not in exchange_markets:
                    exchange_markets[exchange] = []
                exchange_markets[exchange].append(symbol)
            except:
                continue

        tasks = []
        for exchange, symbols in exchange_markets.items():
            logger.info(f"  → {exchange}: {len(symbols)} markets")
            task = asyncio.create_task(
                self.subscribe_orderbooks(exchange, symbols, stop_event)
            )
            tasks.append(task)

        self._tasks = tasks

        await asyncio.gather(*tasks, return_exceptions=True)

        self._running = False
        logger.info("✅ WebSocket subscriptions stopped")

    async def close(self) -> None:
        logger.info("Closing WebSocket connections...")

        for task in self._tasks:
            if not task.done():
                task.cancel()

        for name, client in self.exchanges.items():
            try:
                await client.close()
                logger.info(f"  ✓ Closed {name}")
            except Exception as e:
                logger.debug(f"  ✗ Error closing {name}: {e}")

        self.exchanges.clear()
        self._tasks.clear()


async def create_websocket_manager(
    caches: Dict[str, OrderbookCache],
    exchanges: Optional[List[str]] = None
) -> WebSocketManager:
    return WebSocketManager(caches, exchanges)
