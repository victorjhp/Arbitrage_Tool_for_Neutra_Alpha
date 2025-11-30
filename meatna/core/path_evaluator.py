from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from .config import Config, TokenRule
from .market_graph import MarketEdge
from .orderbook_cache import OrderbookSnapshot
from meatna.utils.logging import debug_log

logger = logging.getLogger(__name__)


@dataclass
class LegResult:
    market_code: str
    side: str
    notional_quote: float
    effective_price: float
    input_amount: float
    output_amount: float
    fee_rate: float
    depth_used: Sequence[tuple[float, float]]


@dataclass
class PathEvaluation:
    path_id: str
    starting_amount: float
    final_amount: float
    delta_inst: float
    delta_vol: float
    delta_slip: float
    delta_final: float
    legs: Sequence[LegResult]


@dataclass
class EvaluationDebug:
    path_id: str
    assets: Sequence[str]
    starting_amount: float
    final_amount: float
    delta_inst: float
    delta_vol: float
    delta_slip: float
    delta_final: float
    reason: str
    legs: Sequence[LegResult]
    orderbooks: Mapping[str, Dict[str, Sequence[tuple[float, float]]]]


class PathEvaluator:

    def __init__(self, config: Config):
        self._config = config
        self._first_leg_multiplier = config.min_order.first_leg_multiplier
        self._safety_multiplier = config.min_order.min_notional_multiplier

    def evaluate(
        self,
        path_id: str,
        edges: Sequence[MarketEdge],
        assets: Sequence[str],
        starting_notional: float,
        snapshots: Mapping[str, OrderbookSnapshot],
        sigma_by_asset: Mapping[str, float] | None = None,
        *,
        debug: bool = False,
    ) -> Tuple[Optional[PathEvaluation], Optional[EvaluationDebug]]:
        current_amount = starting_notional
        legs: List[LegResult] = []
        reason = ""
        orderbook_views = self._capture_books(edges, snapshots, depth=5) if debug else {}
        total_slippage = 0.0
        sigmas = sigma_by_asset or {}

        if edges:
            required_first_leg = edges[0].min_total * self._first_leg_multiplier
            if starting_notional < required_first_leg:
                reason = (
                    f"starting notional {starting_notional:.8f} below first leg minimum {required_first_leg:.8f}"
                )
                return self._fail(path_id, assets, starting_notional, current_amount, legs, reason, orderbook_views, debug)

        for idx, edge in enumerate(edges):
            if current_amount <= 0:
                debug_log(logger, "Zero notional before %s amount=%.12f", edge.market_code, current_amount)
                reason = "Leg received zero notional"
                return self._fail(path_id, assets, starting_notional, current_amount, legs, reason, orderbook_views, debug)
            assert current_amount > 0, f"Zero notional entering leg {edge.market_code}"
            snapshot = snapshots.get(edge.market_code)
            if not snapshot:
                reason = f"missing snapshot for {edge.market_code}"
                return self._fail(path_id, assets, starting_notional, current_amount, legs, reason, orderbook_views, debug)
            if not self._validate_snapshot(edge.market_code, snapshot):
                reason = f"invalid snapshot for {edge.market_code}"
                return self._fail(path_id, assets, starting_notional, current_amount, legs, reason, orderbook_views, debug)
            if not self._has_input_for_leg(current_amount, edge, snapshot):
                reason = f"input below minimum for {edge.market_code}"
                return self._fail(path_id, assets, starting_notional, current_amount, legs, reason, orderbook_views, debug)

            multiplier = self._first_leg_multiplier if idx == 0 else self._safety_multiplier
            min_quote_required = edge.min_total * multiplier

            if edge.direction == "buy":
                result, leg_reason, leg_slip = self._simulate_buy(edge, snapshot, current_amount, min_quote_required)
            else:
                result, leg_reason, leg_slip = self._simulate_sell(edge, snapshot, current_amount, min_quote_required)

            if result is None:
                reason = leg_reason or f"unable to execute {edge.market_code}"
                return self._fail(path_id, assets, starting_notional, current_amount, legs, reason, orderbook_views, debug)

            legs.append(result)
            current_amount = result.output_amount
            if current_amount <= 0:
                reason = f"non-positive output after {edge.market_code}"
                return self._fail(path_id, assets, starting_notional, current_amount, legs, reason, orderbook_views, debug)
            total_slippage += leg_slip

            if idx + 1 < len(edges):
                next_edge = edges[idx + 1]
                next_snapshot = snapshots.get(next_edge.market_code)
                if not next_snapshot:
                    reason = f"missing snapshot for {next_edge.market_code}"
                    return self._fail(path_id, assets, starting_notional, current_amount, legs, reason, orderbook_views, debug)
                if not self._validate_snapshot(next_edge.market_code, next_snapshot):
                    reason = f"invalid snapshot for {next_edge.market_code}"
                    return self._fail(path_id, assets, starting_notional, current_amount, legs, reason, orderbook_views, debug)
                if not self._has_input_for_leg(current_amount, next_edge, next_snapshot):
                    reason = f"insufficient size for next leg {next_edge.market_code}"
                    return self._fail(path_id, assets, starting_notional, current_amount, legs, reason, orderbook_views, debug)

        delta_inst = current_amount / starting_notional - 1.0
        delta_vol = self._compute_vol_penalty(assets, sigmas)
        extra_edge = self._extra_edge_requirement(assets)
        min_profit = self._config.risk_model.min_profit_margin + extra_edge
        delta_final = delta_inst - delta_vol - total_slippage
        evaluation = PathEvaluation(
            path_id=path_id,
            starting_amount=starting_notional,
            final_amount=current_amount,
            delta_inst=delta_inst,
            delta_vol=delta_vol,
            delta_slip=total_slippage,
            delta_final=delta_final,
            legs=legs,
        )
        debug_info = None
        if debug:
            debug_info = self._build_debug(
                path_id,
                assets,
                starting_notional,
                current_amount,
                legs,
                reason or "evaluated",
                delta_inst,
                delta_vol,
                total_slippage,
                orderbook_views,
            )
        if delta_final <= min_profit:
            return (None, debug_info) if debug else (None, None)
        return evaluation, (debug_info if debug else None)

    def _simulate_buy(
        self,
        edge: MarketEdge,
        snapshot: OrderbookSnapshot,
        quote_amount: float,
        min_quote_required: float,
    ) -> Tuple[Optional[LegResult], Optional[str], float]:
        assert quote_amount > 0, f"Zero notional entering leg {edge.market_code}"
        remaining = quote_amount
        acquired = 0.0
        spent = 0.0
        depth_used: List[tuple[float, float]] = []
        for level in snapshot.asks:
            price = level.price
            size = level.size
            assert price > 0, f"Zero-price detected in {edge.market_code}"
            if size <= 0:
                continue
            cost = price * size
            if cost <= remaining:
                acquired += size
                spent += cost
                depth_used.append((price, size))
                remaining -= cost
            else:
                size_partial = remaining / price
                acquired += size_partial
                spent += remaining
                depth_used.append((price, size_partial))
                remaining = 0.0
                break
        if remaining > 1e-9 or acquired <= 0:
            return None, "insufficient ask depth", 0.0
        if spent < min_quote_required:
            return None, f"notional {spent:.8f} below minimum {min_quote_required:.8f}", 0.0
        fee_rate = edge.bid_fee
        vwap = spent / acquired
        if vwap <= 0:
            return None, "invalid VWAP", 0.0
        effective_price = vwap * (1 + fee_rate)
        slippage_penalty = self._buy_slippage(snapshot, effective_price)
        debug_log(
            logger,
            "leg %s side=%s in=%f out=%f vwap=%f",
            edge.market_code,
            "buy",
            quote_amount,
            acquired,
            effective_price,
        )
        result = LegResult(
            market_code=edge.market_code,
            side="buy",
            notional_quote=spent,
            effective_price=effective_price,
            input_amount=quote_amount,
            output_amount=acquired,
            fee_rate=fee_rate,
            depth_used=tuple(depth_used),
        )
        return result, None, slippage_penalty

    def _simulate_sell(
        self,
        edge: MarketEdge,
        snapshot: OrderbookSnapshot,
        base_amount: float,
        min_quote_required: float,
    ) -> Tuple[Optional[LegResult], Optional[str], float]:
        assert base_amount > 0, f"Zero notional entering leg {edge.market_code}"
        remaining = base_amount
        proceeds = 0.0
        depth_used: List[tuple[float, float]] = []
        for level in snapshot.bids:
            price = level.price
            size = level.size
            assert price > 0, f"Zero-price detected in {edge.market_code}"
            if size <= 0:
                continue
            if size <= remaining:
                proceeds += price * size
                depth_used.append((price, size))
                remaining -= size
            else:
                proceeds += price * remaining
                depth_used.append((price, remaining))
                remaining = 0.0
                break
        if remaining > 1e-9:
            return None, "insufficient bid depth", 0.0
        if proceeds < min_quote_required:
            return None, f"notional {proceeds:.8f} below minimum {min_quote_required:.8f}", 0.0
        fee_rate = edge.ask_fee
        vwap = proceeds / base_amount if base_amount > 0 else 0.0
        if vwap <= 0:
            return None, "invalid VWAP", 0.0
        effective_price = vwap * (1 - fee_rate)
        slippage_penalty = self._sell_slippage(snapshot, effective_price)
        debug_log(
            logger,
            "leg %s side=%s in=%f out=%f vwap=%f",
            edge.market_code,
            "sell",
            base_amount,
            proceeds,
            effective_price,
        )
        result = LegResult(
            market_code=edge.market_code,
            side="sell",
            notional_quote=proceeds,
            effective_price=effective_price,
            input_amount=base_amount,
            output_amount=proceeds,
            fee_rate=fee_rate,
            depth_used=tuple(depth_used),
        )
        return result, None, slippage_penalty

    def _has_input_for_leg(self, amount_available: float, edge: MarketEdge, snapshot: OrderbookSnapshot) -> bool:
        required = edge.min_total * self._safety_multiplier
        if required <= 0:
            return True
        if edge.side == "buy":
            return amount_available >= required
        best_bid = snapshot.bids[0].price if snapshot.bids else 0.0
        if best_bid <= 0:
            debug_log(logger, "Invalid best bid for %s", edge.market_code)
            return False
        estimated_quote = amount_available * best_bid
        return estimated_quote >= required

    def _validate_snapshot(self, market_code: str, snapshot: OrderbookSnapshot) -> bool:
        if not snapshot.bids or not snapshot.asks:
            debug_log(logger, "Empty orderbook for %s", market_code)
            return False
        best_bid = snapshot.bids[0].price
        best_ask = snapshot.asks[0].price
        if best_bid <= 0 or best_ask <= 0:
            debug_log(logger, "Non-positive best levels for %s bid=%s ask=%s", market_code, best_bid, best_ask)
            return False
        return True

    def _compute_vol_penalty(self, assets: Sequence[str], sigma_by_asset: Mapping[str, float]) -> float:
        relevant = [asset for asset in assets if asset != "USD"]
        if not relevant:
            return 0.0
        sigma = max((sigma_by_asset.get(asset, 0.0) for asset in relevant), default=0.0)
        return self._config.risk_model.vol_risk_multiplier * sigma

    def _buy_slippage(self, snapshot: OrderbookSnapshot, effective_price: float) -> float:
        best_ask = snapshot.asks[0].price if snapshot.asks else 0.0
        if best_ask <= 0:
            return 0.0
        return max(0.0, (effective_price - best_ask) / best_ask)

    def _sell_slippage(self, snapshot: OrderbookSnapshot, effective_price: float) -> float:
        best_bid = snapshot.bids[0].price if snapshot.bids else 0.0
        if best_bid <= 0:
            return 0.0
        return max(0.0, (best_bid - effective_price) / best_bid)

    def _extra_edge_requirement(self, assets: Sequence[str]) -> float:
        extra = 0.0
        for asset in assets:
            token: TokenRule | None = self._config.tokens.get(asset)
            if not token:
                continue
            extra = max(extra, token.extra_edge_required)
        return extra

    def _capture_books(
        self, edges: Sequence[MarketEdge], snapshots: Mapping[str, OrderbookSnapshot], depth: int = 3
    ) -> Dict[str, Dict[str, Sequence[tuple[float, float]]]]:
        views: Dict[str, Dict[str, Sequence[tuple[float, float]]]] = {}
        for edge in edges:
            snapshot = snapshots.get(edge.market_code)
            if not snapshot:
                continue
            bids = [(level.price, level.size) for level in snapshot.bids[:depth]]
            asks = [(level.price, level.size) for level in snapshot.asks[:depth]]
            views[edge.market_code] = {"bids": bids, "asks": asks}
        return views

    def _build_debug(
        self,
        path_id: str,
        assets: Sequence[str],
        starting: float,
        final: float,
        legs: Sequence[LegResult],
        reason: str,
        delta_inst: float,
        delta_vol: float,
        delta_slip: float,
        orderbooks: Mapping[str, Dict[str, Sequence[tuple[float, float]]]],
    ) -> EvaluationDebug:
        delta_final = delta_inst - delta_vol - delta_slip
        return EvaluationDebug(
            path_id=path_id,
            assets=assets,
            starting_amount=starting,
            final_amount=final,
            delta_inst=delta_inst,
            delta_vol=delta_vol,
            delta_slip=delta_slip,
            delta_final=delta_final,
            reason=reason,
            legs=tuple(legs),
            orderbooks=orderbooks,
        )

    def _fail(
        self,
        path_id: str,
        assets: Sequence[str],
        starting_notional: float,
        current_amount: float,
        legs: Sequence[LegResult],
        reason: str,
        orderbook_views: Mapping[str, Dict[str, Sequence[tuple[float, float]]]],
        debug: bool,
    ) -> Tuple[None, Optional[EvaluationDebug]]:
        if not debug:
            return None, None
        debug_info = self._build_debug(
            path_id,
            assets,
            starting_notional,
            current_amount,
            legs,
            reason,
            0.0,
            0.0,
            0.0,
            orderbook_views,
        )
        return None, debug_info
