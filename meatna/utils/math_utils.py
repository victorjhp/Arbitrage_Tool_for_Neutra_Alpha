from __future__ import annotations

import math
from statistics import pstdev
from typing import Iterable, Sequence


def log_return(current: float, previous: float) -> float:
    if current <= 0 or previous <= 0:
        return 0.0
    return math.log(current / previous)


def stddev(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return 0.0
    return pstdev(values)


def sum_top_levels(levels: Iterable, depth: int) -> float:
    total = 0.0
    for level in list(levels)[:depth]:
        size = level[1] if isinstance(level, (tuple, list)) else getattr(level, "size", 0.0)
        total += float(size)
    return total
