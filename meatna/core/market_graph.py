from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Iterable, Union

from meatna.exchange.models import MarketInfo


@dataclass(frozen=True)
class MarketEdge:
    market_code: str
    from_asset: str
    to_asset: str
    side: str
    fee_rate: float
    min_total: float
    is_reversed: bool
    exchange: str = ""

    @property
    def direction(self) -> str:
        return self.side

    @property
    def bid_fee(self) -> float:
        return self.fee_rate

    @property
    def ask_fee(self) -> float:
        return self.fee_rate


class MarketGraph:

    def __init__(self, markets: Union[Dict[str, MarketInfo], List[MarketInfo]], config=None):

        if isinstance(markets, list):
            markets = {m.market: m for m in markets}

        self._edges: List[MarketEdge] = []
        self._by_source: Dict[str, List[MarketEdge]] = {}
        self._min_order = getattr(config, "min_order", None)

        for market_key, m in markets.items():
            quote = m.quote_currency
            base = m.base_currency
            base_market_code = m.market

            exchange = ""
            if "::" in market_key:
                exchange = market_key.split("::")[0]
                market_code = market_key
            else:
                market_code = base_market_code

            buy_min_total = self._compute_min_total(quote)
            buy_edge = MarketEdge(
                market_code=market_code,
                from_asset=quote,
                to_asset=base,
                side="buy",
                fee_rate=0.0004,
                min_total=buy_min_total,
                is_reversed=False,
                exchange=exchange,
            )
            self._add_edge(buy_edge)

            sell_min_total = self._compute_min_total(quote)
            sell_edge = MarketEdge(
                market_code=market_code,
                from_asset=base,
                to_asset=quote,
                side="sell",
                fee_rate=0.0004,
                min_total=sell_min_total,
                is_reversed=True,
                exchange=exchange,
            )
            self._add_edge(sell_edge)

    def _compute_min_total(self, quote: str) -> float:
        if not self._min_order:
            return 1.0

        base_min = self._min_order.quote_min_notional.get(quote, 1.0)
        multiplier = self._min_order.min_notional_multiplier
        return float(base_min) * float(multiplier)

    def _add_edge(self, edge: MarketEdge) -> None:
        self._edges.append(edge)
        self._by_source.setdefault(edge.from_asset, []).append(edge)

    def out_edges(self, asset: str) -> Iterable[MarketEdge]:
        return self._by_source.get(asset, [])

    @property
    def edges(self) -> List[MarketEdge]:
        return self._edges

    def all_markets(self) -> List[str]:
        return list({edge.market_code for edge in self._edges})
