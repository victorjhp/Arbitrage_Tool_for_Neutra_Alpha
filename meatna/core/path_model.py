from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence, Set

from .market_graph import MarketGraph, MarketEdge
from .config import Config


@dataclass(frozen=True)
class PathDefinition:
    path_id: str
    edges: Sequence[MarketEdge]
    assets: Sequence[str]


class PathModel:

    def __init__(self, graph: MarketGraph, config: Config) -> None:
        self._graph = graph
        self._config = config
        self._paths = self._build_paths()

    @property
    def paths(self) -> Sequence[PathDefinition]:
        return tuple(self._paths)

    def markets_in_use(self) -> Set[str]:
        mk = set()
        for path in self._paths:
            for e in path.edges:
                mk.add(e.market_code)
        return mk

    def assets_in_paths(self) -> Set[str]:
        a = set()
        for p in self._paths:
            a.update(p.assets)
        return a

    def _build_paths(self) -> Sequence[PathDefinition]:
        start_asset = "USD"
        paths: List[PathDefinition] = []
        tokens = self._config.tokens
        path_id = 0

        def dfs(
            current_asset: str,
            edges: List[MarketEdge],
            assets: List[str],
            visited: Set[str],
            must_return_to_usd: bool,
        ) -> None:
            nonlocal path_id

            if len(edges) >= self._config.paths.max_length:
                return

            for edge in self._graph.out_edges(current_asset):

                if must_return_to_usd and edge.to_asset != start_asset:
                    continue

                next_asset = edge.to_asset
                token_rule = tokens.get(next_asset)
                if next_asset != start_asset:
                    if not token_rule or not token_rule.enabled:
                        continue
                    if not token_rule.allowed_as_terminal_asset and not token_rule.allowed_as_bridge:
                        continue

                if (
                    not self._config.paths.allow_revisit_nodes
                    and next_asset in visited
                    and next_asset != start_asset
                ):
                    continue

                edges.append(edge)
                assets.append(next_asset)

                if next_asset == start_asset and len(edges) >= self._config.paths.min_length:
                    path_id += 1
                    paths.append(
                        PathDefinition(
                            path_id=f"path_{path_id}",
                            edges=tuple(edges),
                            assets=tuple(assets),
                        )
                    )

                elif len(edges) < self._config.paths.max_length:
                    if next_asset != start_asset:
                        visited.add(next_asset)

                    require_return = False
                    if next_asset != start_asset:
                        rule = tokens.get(next_asset)
                        require_return = not rule.allowed_as_bridge

                    dfs(next_asset, edges, assets, visited, require_return)

                    if next_asset != start_asset:
                        visited.discard(next_asset)

                edges.pop()
                assets.pop()

        dfs(start_asset, [], [start_asset], set(), False)
        return paths


__all__ = ["PathModel", "PathDefinition"]
