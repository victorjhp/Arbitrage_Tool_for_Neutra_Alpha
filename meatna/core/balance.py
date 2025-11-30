from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class QuoteBalances:
    usd: float
    btc: float
    usdt: float
    usdc: float = 0.0
