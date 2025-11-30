from __future__ import annotations

import asyncio
import math
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterable

from meatna.exchange.models import Ticker

from .config import Config, DEFAULT_SIGMA_BY_TIER, TokenRule
from ..utils.math_utils import log_return


@dataclass
class PriceSample:
    timestamp_ms: int
    price: float


class VolatilityCache:

    def __init__(self, config: Config) -> None:
        self._config = config
        self._samples: Dict[str, Deque[PriceSample]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def update_from_ticker(self, ticker: Ticker) -> None:
        market = ticker.market
        if "-" not in market:
            return
        quote, base = market.split("-")
        if quote != "KRW":
            return
        price = float(ticker.trade_price)
        timestamp = int(ticker.timestamp)
        sample = PriceSample(timestamp_ms=timestamp, price=price)
        async with self._lock:
            window = self._samples[base]
            window.append(sample)
            self._prune(window, timestamp)

    def _prune(self, window: Deque[PriceSample], now: int) -> None:
        cutoff = now - int(self._config.risk_model.volatility_window_seconds * 1000)
        while window and window[0].timestamp_ms < cutoff:
            window.popleft()

    async def get_sigma(self, asset: str) -> float:
        async with self._lock:
            window = self._samples.get(asset)
            return self._sigma_from_window(asset, window)

    async def snapshot_sigmas(self, assets: Iterable[str]) -> Dict[str, float]:
        async with self._lock:
            return {asset: self._sigma_from_window(asset, self._samples.get(asset)) for asset in assets}

    def _default_sigma(self, asset: str) -> float:
        token_rule: TokenRule | None = self._config.tokens.get(asset)
        tier = token_rule.volatility_tier if token_rule else 3
        return DEFAULT_SIGMA_BY_TIER.get(tier, 0.005)

    def _sigma_from_window(self, asset: str, window: Deque[PriceSample] | None) -> float:
        if not window or len(window) < 2:
            return self._default_sigma(asset)
        samples = list(window)
        returns: list[float] = []
        deltas: list[float] = []
        for previous, current in zip(samples, samples[1:]):
            if current.price <= 0 or previous.price <= 0:
                continue
            returns.append(log_return(current.price, previous.price))
            if current.timestamp_ms > previous.timestamp_ms:
                deltas.append((current.timestamp_ms - previous.timestamp_ms) / 1000)
        if not returns:
            return self._default_sigma(asset)
        avg_delta = (
            sum(deltas) / len(deltas) if deltas else self._config.risk_model.volatility_sampling_interval_seconds
        )
        variance = 0.0
        mean = sum(returns) / len(returns)
        for value in returns:
            variance += (value - mean) ** 2
        variance /= len(returns)
        sigma = math.sqrt(max(variance, 0.0))
        if avg_delta <= 0:
            avg_delta = self._config.risk_model.volatility_sampling_interval_seconds
        sigma_per_sec = sigma / math.sqrt(avg_delta)
        return float(sigma_per_sec)

    async def has_data(self) -> bool:
        async with self._lock:
            return any(self._samples.values())
