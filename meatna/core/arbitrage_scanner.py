from __future__ import annotations

import asyncio
import logging
from typing import Dict, Iterable, Mapping, Sequence

from meatna.core.config_loader import Config
from meatna.core.balance import QuoteBalances
from meatna.core.orderbook_cache import OrderbookCache, OrderbookSnapshot
from meatna.core.volatility_cache import VolatilityCache
from meatna.core.path_model import PathModel
from meatna.core.path_evaluator import PathEvaluator, PathEvaluation, EvaluationDebug
from meatna.utils.logging import debug_log

logger = logging.getLogger(__name__)


class ArbitrageScannerMulti:

    def __init__(
        self,
        config: Config,
        path_model: PathModel,
        caches: Dict[str, OrderbookCache],
        balances: Dict[str, QuoteBalances],
        volatility_cache: VolatilityCache | None = None,
    ):
        self._config = config
        self._path_model = path_model
        self._caches = caches
        self._balances = balances
        self._vol = volatility_cache
        self._evaluator = PathEvaluator(config)
        self._best_delta = float("-inf")
        self._best_record = None

    async def run_once(self) -> dict | None:

        for ex, cache in self._caches.items():
            if not await cache.has_data():
                logger.info("Waiting for initial orderbook for %s...", ex)
                return None

        start_usdc = sum([b.usdc for b in self._balances.values()])
        if start_usdc <= 0:
            return None

        start = asyncio.get_running_loop().time()
        evaluated = 0
        ops = 0
        sigma_map = await self._snapshot_sigmas()

        for path in self._path_model.paths:

            snapshots: Dict[str, OrderbookSnapshot] = {}

            missing = False
            for leg in path.edges:
                cache = self._caches.get(leg.exchange)
                if not cache:
                    missing = True
                    break
                snap = await cache.snapshot(leg.market_code)
                if not snap:
                    missing = True
                    break
                snapshots[leg.market_code] = snap

            if missing:
                continue

            evaluated += 1
            result, dbg = self._evaluator.evaluate(
                path_id=path.path_id,
                edges=path.edges,
                assets=path.assets,
                starting_notional=start_usdc,
                snapshots=snapshots,
                sigma_by_asset=sigma_map,
                debug=False,
            )

            if result:
                ops += 1
                self._update_best(path, result)

        dur = (asyncio.get_running_loop().time() - start) * 1000
        return {
            "evaluated": evaluated,
            "opportunities": ops,
            "duration_ms": round(dur, 2),
            **self._best_summary(),
        }

    async def _snapshot_sigmas(self) -> Mapping[str, float]:
        if not self._vol:
            return {}
        return await self._vol.snapshot_sigmas(self._path_model.assets_in_paths())

    def _update_best(self, path, eval: PathEvaluation):
        if eval.delta_final <= self._best_delta:
            return
        self._best_delta = eval.delta_final
        self._best_record = (path.path_id, path.assets, eval)

    def _best_summary(self):
        if not self._best_record:
            return {
                "best_path_id": None,
                "best_assets": None,
                "best_delta_final": None,
            }
        pid, assets, ev = self._best_record
        return {
            "best_path_id": pid,
            "best_assets": list(assets),
            "best_delta_final": ev.delta_final,
        }
