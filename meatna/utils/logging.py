from __future__ import annotations

import logging
from typing import Iterable

TRACE_LEVEL = logging.DEBUG - 5
logging.addLevelName(TRACE_LEVEL, "TRACE")

_DEBUG_ENABLED = False


def setup_logging(*, debug_mode: bool, modules: Iterable[str] | None = None) -> None:
    global _DEBUG_ENABLED
    _DEBUG_ENABLED = bool(debug_mode)
    level = logging.DEBUG if _DEBUG_ENABLED else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    if modules:
        for name in modules:
            logging.getLogger(name).setLevel(level)


def is_debug_enabled() -> bool:
    return _DEBUG_ENABLED


def debug_log(logger: logging.Logger, msg: str, *args, **kwargs) -> None:
    if _DEBUG_ENABLED:
        logger.debug(msg, *args, **kwargs)


def trace_log(logger: logging.Logger, msg: str, *args, **kwargs) -> None:
    if _DEBUG_ENABLED:
        logger.log(TRACE_LEVEL, msg, *args, **kwargs)
