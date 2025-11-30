from __future__ import annotations

import asyncio
import logging
from typing import List, Sequence

from arbbot.core.config_loader import ConfigLoader, Config
from arbbot.core.market_graph import MarketGraph, MarketEdge
from arbbot.core.orderbook_cache import OrderbookCache
from arbbot.core.path_model import PathModel
from arbbot.core.path_evaluator import PathEvaluator
from arbbot.core.scanner import ArbitrageScanner, ExecutionCoordinator
from arbbot.core.balance import QuoteBalances
from arbbot.core.volatility_cache import VolatilityCache
from arbbot.infra.account_service import AccountService
from arbbot.infra.rest_client_ccxt import RestBootstrapper
from arbbot.infra.ws_manager_ccxt import WSManager
from arbbot.utils.logging import setup_logging
from arbbot.exchange.models import MarketInfo


logger = logging.getLogger(__name__)


def _filter_markets(markets: Sequence[MarketInfo], config: Config) -> List[MarketInfo]:
    allowed_quotes = set(config.min_order.quote_min_notional.keys())
    enabled_tokens = {symbol for symbol, token in config.tokens.items() if token.enabled}
    filtered: List[MarketInfo] = []
    for market in markets:
        if "-" not in market.market:
            continue
        quote, base = market.market.split("-")
        if quote not in allowed_quotes:
            continue
        if base not in enabled_tokens:
            continue
        filtered.append(market)
    return filtered


def _find_edge(graph: MarketGraph, from_asset: str, to_asset: str) -> MarketEdge:
    for edge in graph.edges:
        if edge.from_asset == from_asset and edge.to_asset == to_asset:
            return edge
    raise ValueError(f"Missing edge {from_asset}->{to_asset}")


async def _run_self_test(
    graph: MarketGraph,
    orderbook_cache: OrderbookCache,
    config: Config,
    balances: QuoteBalances,
) -> None:
    evaluator = PathEvaluator(config)
    await _run_roundtrip_test(
        label="KRW→USDC→KRW",
        graph=graph,
        orderbook_cache=orderbook_cache,
        evaluator=evaluator,
        from_asset="KRW",
        via_asset="USDC",
        balances_available=balances.krw,
        max_trade=config.trade_sizing.max_krw_per_trade,
        min_quote=config.min_order.quote_min_notional.get("KRW", 5000.0),
        expected_band=(-0.0045, -0.002),
    )
    await _run_roundtrip_test(
        label="USDC→BTC→USDC",
        graph=graph,
        orderbook_cache=orderbook_cache,
        evaluator=evaluator,
        from_asset="USDC",
        via_asset="BTC",
        balances_available=balances.usdt,
        max_trade=config.trade_sizing.max_usdt_per_trade,
        min_quote=config.min_order.quote_min_notional.get("USDT", 5.0),
        expected_band=(-0.03, -0.0001),
    )


async def _run_roundtrip_test(
    *,
    label: str,
    graph: MarketGraph,
    orderbook_cache: OrderbookCache,
    evaluator: PathEvaluator,
    from_asset: str,
    via_asset: str,
    balances_available: float,
    max_trade: float,
    min_quote: float,
    expected_band: tuple[float, float],
) -> None:
    try:
        forward = _find_edge(graph, from_asset, via_asset)
        backward = _find_edge(graph, via_asset, from_asset)
    except ValueError as exc:
        logger.warning("Self-test %s skipped: %s", label, exc)
        return
    start_amount = min(balances_available, max_trade, min_quote * 4)
    if start_amount <= 0:
        logger.warning("Self-test %s skipped: insufficient starting capital", label)
        return
    markets = {forward.market_code, backward.market_code}
    snapshots = await orderbook_cache.snapshot_many(markets)
    if len(snapshots) != len(markets):
        missing = markets - set(snapshots.keys())
        logger.warning("Self-test %s skipped: missing snapshots for %s", label, ", ".join(sorted(missing)))
        return
    result, debug_info = evaluator.evaluate(
        path_id=f"self_test_{from_asset.lower()}_{via_asset.lower()}",
        edges=[forward, backward],
        assets=[from_asset, via_asset, from_asset],
        starting_notional=start_amount,
        snapshots=snapshots,
        debug=True,
    )
    if result:
        delta = result.delta_inst
    elif debug_info:
        delta = debug_info.delta_inst
        logger.info("Self-test %s lacked profitable result (reason=%s); using debug delta.", label, debug_info.reason)
    else:
        raise RuntimeError(f"Self-test {label} failed: no evaluation data")
    logger.info(
        "Self-test %s Δinst=%.4f expected_band=[%.4f, %.4f]",
        label,
        delta,
        expected_band[0],
        expected_band[1],
    )
    if label == "KRW→USDC→KRW":
        snapshot = snapshots.get(forward.market_code)
        if snapshot and snapshot.bids and snapshot.asks:
            best_bid = snapshot.bids[0].price
            best_ask = snapshot.asks[0].price
            mid = (best_bid + best_ask) / 2
            half_spread = ((best_ask - best_bid) / mid) / 2 if mid > 0 else 0.0
            fee = forward.bid_fee
            expected = -(2 * half_spread + 2 * fee)
            logger.info(
                "  implied Δinst≈%.4f (half_spread=%.6f fee=%.6f)",
                expected,
                half_spread,
                fee,
            )
    if delta > 0 or delta < -0.03:
        raise RuntimeError(f"Self-test {label} Δinst {delta:.4f} outside safe [-0.03, 0]. Aborting.")
    if not (expected_band[0] <= delta <= expected_band[1]):
        logger.warning(
            "Self-test %s Δinst %.4f outside expected band [%.4f, %.4f]",
            label,
            delta,
            expected_band[0],
            expected_band[1],
        )


async def async_main() -> None:
    config = ConfigLoader().load()
    setup_logging(debug_mode=config.logging.debug_mode)
    orderbook_cache = OrderbookCache()
    vol_cache = VolatilityCache(config)

    async with RestBootstrapper() as rest:
        markets = await rest.fetch_markets()
        filtered_markets = _filter_markets(markets, config)
        if not filtered_markets:
            raise RuntimeError("No markets available for configured tokens")
        graph = MarketGraph.build(filtered_markets, config)
        path_model = PathModel(graph, config)
        orderbook_markets = sorted(path_model.markets_in_use())
        ticker_markets = sorted({market for market in orderbook_markets if market.startswith("KRW-")})
        seed_books = await rest.fetch_orderbooks(orderbook_markets)
        if seed_books:
            for book in seed_books:
                await orderbook_cache.update(book)
            logger.info("Seeded %d orderbooks via REST before WS warm-up", len(seed_books))
        else:
            logger.warning("No orderbooks returned from REST seeding step")

    if not path_model.paths:
        logger.warning("No KRW-anchored paths generated. Update token configuration.")

    logger.info(
        "Initialized path model with %d paths across %d markets (%d ticker markets)",
        len(path_model.paths),
        len(orderbook_markets),
        len(ticker_markets),
    )

    async with AccountService() as account_service:
        balances = await account_service.fetch_balances()
    logger.info(
        "Available balances KRW=%.0f BTC=%.6f USDT=%.2f",
        balances.krw,
        balances.btc,
        balances.usdt,
    )
    await _run_self_test(graph, orderbook_cache, config, balances)

    ws_manager = WSManager(
        orderbook_cache,
        vol_cache,
        orderbook_markets=orderbook_markets,
        ticker_markets=ticker_markets,
        debug=config.logging.debug_mode,
    )
    execution = ExecutionCoordinator()
    scanner = ArbitrageScanner(
        config,
        path_model,
        orderbook_cache,
        execution,
        balances,
        vol_cache,
    )

    await ws_manager.start()
    try:
        await scanner.run()
    finally:
        await ws_manager.stop()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:  # pragma: no cover - interactive convenience
        logger.info("Shutting down arbitrage bot")


if __name__ == "__main__":
    main()
