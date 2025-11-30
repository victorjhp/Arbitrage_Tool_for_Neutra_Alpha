from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, List
import yaml


DEFAULT_SIGMA_BY_TIER = {
    0: 0.0003,
    1: 0.0005,
    2: 0.0015,
    3: 0.003,
    4: 0.005,
    5: 0.01,
}


def load_secrets(path: str | Path = None) -> Dict[str, Any]:
    if path is None:
        root = Path(__file__).resolve().parent.parent.parent
        path = root / "config" / "secrets.yaml"

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"secrets.yaml not found at: {path}")

    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}



@dataclass(frozen=True)
class BotModeConfig:
    dry_run: bool


@dataclass(frozen=True)
class MinOrderConfig:
    quote_min_notional: Mapping[str, float]
    min_notional_multiplier: float
    first_leg_multiplier: float


@dataclass(frozen=True)
class AssetConfig:
    role: str
    enabled: bool


@dataclass(frozen=True)
class TokenRule:
    tier: int
    enabled: bool
    allowed_as_bridge: bool
    allowed_as_terminal_asset: bool
    volatility_tier: int
    risk_level: str | None = None
    extra_edge_required: float = 0.0


@dataclass(frozen=True)
class PathsConfig:
    min_length: int
    max_length: int
    allow_revisit_nodes: bool
    extra_leg_min_edge_improvement: float


@dataclass(frozen=True)
class RiskModelConfig:
    volatility_window_seconds: float
    volatility_sampling_interval_seconds: float
    vol_risk_multiplier: float

    slippage_top_levels: int
    slippage_coefficient: float
    min_profit_margin: float


@dataclass(frozen=True)
class LatencyModelConfig:
    rolling_samples: int = 30
    spike_std_multiplier: float = 3.0
    default_leg_time_seconds: float = 0.2


@dataclass(frozen=True)
class TradeSizingConfig:
    starting_size_fractions: List[float]
    max_fraction_of_equity_per_path: float


@dataclass(frozen=True)
class ExecutionConfig:
    inter_leg_timeout_seconds: float
    max_concurrent_paths: int


@dataclass(frozen=True)
class FailSafesConfig:
    stop_on_inconsistent_balance: bool
    max_daily_loss_fraction: float
    max_api_error_rate: float
    pause_after_rate_limit_seconds: int


@dataclass(frozen=True)
class LoggingConfig:
    log_trades: bool
    log_paths: bool
    log_risk_calcs: bool
    print_dry_run_actions: bool
    log_profitable_trades: bool
    heartbeat_enabled: bool
    debug_mode: bool


@dataclass(frozen=True)
class ScannerConfig:
    scan_interval_ms: int


@dataclass(frozen=True)
class Config:
    bot_mode: BotModeConfig
    min_order: MinOrderConfig
    assets: Mapping[str, AssetConfig]
    tokens: Mapping[str, TokenRule]
    paths: PathsConfig
    risk_model: RiskModelConfig
    latency_model: LatencyModelConfig
    trade_sizing: TradeSizingConfig
    execution: ExecutionConfig
    failsafes: FailSafesConfig
    logging: LoggingConfig
    scanner: ScannerConfig


class ConfigLoader:
    def __init__(self, path: str | Path = "config.yaml"):
        self._path = Path(path)

    def load(self) -> Config:
        if not self._path.exists():
            raise FileNotFoundError(f"Config file not found: {self._path}")

        with self._path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        bot_mode = BotModeConfig(
            dry_run=bool(raw.get("bot_mode", {}).get("dry_run", True))
        )

        mo = raw.get("min_order", {})

        quote_min_notional = dict(mo.get("quote_min_notional", {}))
        defaults = {"USD": 5.0, "USDT": 5.0, "USDC": 5.0, "BTC": 0.0002}
        for k, v in defaults.items():
            quote_min_notional.setdefault(k, v)

        min_order = MinOrderConfig(
            quote_min_notional=quote_min_notional,
            min_notional_multiplier=float(mo.get("min_notional_multiplier", 1.0)),
            first_leg_multiplier=float(mo.get("first_leg_multiplier", 1.0)),
        )

        assets_raw = raw.get("assets", {})
        assets = {
            a: AssetConfig(
                role=cfg.get("role", ""),
                enabled=bool(cfg.get("enabled", False)),
            )
            for a, cfg in assets_raw.items()
        }

        tokens_raw = raw.get("tokens", {})
        tokens = {
            name: TokenRule(
                tier=int(cfg.get("tier", 3)),
                enabled=bool(cfg.get("enabled", False)),
                allowed_as_bridge=bool(cfg.get("allowed_as_bridge", False)),
                allowed_as_terminal_asset=bool(cfg.get("allowed_as_terminal_asset", False)),
                volatility_tier=int(cfg.get("volatility_tier", 3)),
                risk_level=cfg.get("risk_level"),
                extra_edge_required=float(cfg.get("extra_edge_required", 0.0)),
            )
            for name, cfg in tokens_raw.items()
        }

        p = raw.get("paths", {})
        paths = PathsConfig(
            min_length=int(p.get("min_length", 2)),
            max_length=int(p.get("max_length", 4)),
            allow_revisit_nodes=bool(p.get("allow_revisit_nodes", False)),
            extra_leg_min_edge_improvement=float(p.get("extra_leg_min_edge_improvement", 0.0)),
        )

        r = raw.get("risk_model", {})
        risk_model = RiskModelConfig(
            volatility_window_seconds=float(r.get("volatility_window_seconds", 60)),
            volatility_sampling_interval_seconds=float(r.get("volatility_sampling_interval_seconds", 1)),
            vol_risk_multiplier=float(r.get("vol_risk_multiplier", 0.5)),
            slippage_top_levels=int(r.get("slippage_top_levels", 3)),
            slippage_coefficient=float(r.get("slippage_coefficient", 0.00001)),
            min_profit_margin=float(r.get("min_profit_margin", 0.0)),
        )

        l = raw.get("latency_model", {})
        latency_model = LatencyModelConfig(
            rolling_samples=int(l.get("rolling_samples", 30)),
            spike_std_multiplier=float(l.get("spike_std_multiplier", 3.0)),
            default_leg_time_seconds=float(l.get("default_leg_time_seconds", 0.2)),
        )

        t = raw.get("trade_sizing", {})
        trade_sizing = TradeSizingConfig(
            starting_size_fractions=list(map(float, t.get("starting_size_fractions", []))),
            max_fraction_of_equity_per_path=float(t.get("max_fraction_of_equity_per_path", 1.0)),
        )

        e = raw.get("execution", {})
        execution = ExecutionConfig(
            inter_leg_timeout_seconds=float(e.get("inter_leg_timeout_seconds", 1.0)),
            max_concurrent_paths=int(e.get("max_concurrent_paths", 1)),
        )

        fs = raw.get("failsafes", {})
        failsafes = FailSafesConfig(
            stop_on_inconsistent_balance=bool(fs.get("stop_on_inconsistent_balance", True)),
            max_daily_loss_fraction=float(fs.get("max_daily_loss_fraction", 0.03)),
            max_api_error_rate=float(fs.get("max_api_error_rate", 0.05)),
            pause_after_rate_limit_seconds=int(fs.get("pause_after_rate_limit_seconds", 10)),
        )

        lg = raw.get("logging", {})
        logging_config = LoggingConfig(
            log_trades=bool(lg.get("log_trades", True)),
            log_paths=bool(lg.get("log_paths", False)),
            log_risk_calcs=bool(lg.get("log_risk_calcs", True)),
            print_dry_run_actions=bool(lg.get("print_dry_run_actions", True)),
            log_profitable_trades=bool(lg.get("log_profitable_trades", True)),
            heartbeat_enabled=bool(lg.get("heartbeat_enabled", True)),
            debug_mode=bool(lg.get("debug_mode", True)),
        )

        sc = raw.get("scanner", {})
        scanner = ScannerConfig(
            scan_interval_ms=int(sc.get("scan_interval_ms", 100)),
        )

        return Config(
            bot_mode=bot_mode,
            min_order=min_order,
            assets=assets,
            tokens=tokens,
            paths=paths,
            risk_model=risk_model,
            latency_model=latency_model,
            trade_sizing=trade_sizing,
            execution=execution,
            failsafes=failsafes,
            logging=logging_config,
            scanner=scanner,
        )


def load_config(path: str | Path = "config.yaml") -> Config:
    return ConfigLoader(path).load()
