"""Configuration loading for chq.

Priority (highest wins): CLI flags -> env vars -> YAML file -> dataclass defaults.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import yaml


@dataclass
class Config:
    """All settings for a chq run."""

    # ClickHouse connection
    host: str = ""
    port: int = 8443
    user: str = "default"
    password: str = ""
    secure: bool = True

    # Output
    format: str = "terminal"  # terminal, slack, json, csv
    output_path: str | None = None  # file path for json/csv output
    slack_webhook: str = ""

    # Query parameters (substituted into SQL templates)
    lookback_days: int = 7
    top_n: int = 10
    regression_threshold_pct: int = 50
    min_executions: int = 10
    scan_ratio_threshold: int = 1000
    max_result_rows: int = 100000
    repeat_threshold: int = 100  # per hour
    min_batch_size: int = 1000
    min_parts_threshold: int = 20

    # Category filter (None = all)
    only_categories: list[str] | None = None

    @property
    def sql_params(self) -> dict[str, Any]:
        """Return a dict of query parameters for SQL template substitution."""
        return {
            "lookback_days": self.lookback_days,
            "top_n": self.top_n,
            "regression_threshold_pct": self.regression_threshold_pct,
            "min_executions": self.min_executions,
            "scan_ratio_threshold": self.scan_ratio_threshold,
            "max_result_rows": self.max_result_rows,
            "repeat_threshold": self.repeat_threshold,
            "min_batch_size": self.min_batch_size,
            "min_parts_threshold": self.min_parts_threshold,
        }


# ---------------------------------------------------------------------------
# Human-readable check descriptions (used by --list-checks)
# ---------------------------------------------------------------------------

CHECKS: dict[tuple[str, str], str] = {
    ("top_n", "by_memory"): "Top queries by peak memory usage",
    ("top_n", "by_read_bytes"): "Top queries by total data scanned",
    ("top_n", "by_duration"): "Top queries by p95 execution time",
    ("top_n", "by_weighted_cost"): "Top queries by frequency x average cost",
    ("anomalies", "wow_duration_regressions"): "Queries with week-over-week p95 duration regression",
    ("anomalies", "new_expensive_patterns"): "New query patterns that are already expensive",
    ("cost_attribution", "by_user"): "Resource consumption breakdown by user",
    ("anti_patterns", "full_scans"): "Queries with high read/result row ratio (full scans)",
    ("anti_patterns", "select_star_wide_tables"): "Queries using SELECT * pattern",
    ("anti_patterns", "missing_partition_filter"): "Queries scanning too many parts (missing partition filter)",
    ("anti_patterns", "unbounded_results"): "Queries returning large result sets without LIMIT",
    ("anti_patterns", "repeated_identical"): "Query patterns running excessively often (missing cache)",
    ("anti_patterns", "small_insert_batches"): "INSERT queries with very small batch sizes",
}


# ---------------------------------------------------------------------------
# Environment variable mapping
# ---------------------------------------------------------------------------

_ENV_MAP: dict[str, tuple[str, type]] = {
    "CHQ_HOST": ("host", str),
    "CHQ_PORT": ("port", int),
    "CHQ_USER": ("user", str),
    "CHQ_PASSWORD": ("password", str),
    "CHQ_SECURE": ("secure", bool),
    "CHQ_FORMAT": ("format", str),
    "CHQ_OUTPUT_PATH": ("output_path", str),
    "CHQ_SLACK_WEBHOOK": ("slack_webhook", str),
    "CHQ_LOOKBACK_DAYS": ("lookback_days", int),
    "CHQ_TOP_N": ("top_n", int),
    "CHQ_REGRESSION_THRESHOLD_PCT": ("regression_threshold_pct", int),
    "CHQ_MIN_EXECUTIONS": ("min_executions", int),
    "CHQ_SCAN_RATIO_THRESHOLD": ("scan_ratio_threshold", int),
    "CHQ_MAX_RESULT_ROWS": ("max_result_rows", int),
    "CHQ_REPEAT_THRESHOLD": ("repeat_threshold", int),
    "CHQ_MIN_BATCH_SIZE": ("min_batch_size", int),
    "CHQ_MIN_PARTS_THRESHOLD": ("min_parts_threshold", int),
}


def _coerce(value: str, target_type: type) -> Any:
    """Convert a string env-var value to the expected Python type."""
    if target_type is bool:
        return value.lower() in ("1", "true", "yes")
    return target_type(value)


def load_config(config_path: str | None = None, **cli_overrides: Any) -> Config:
    """Build a Config by layering defaults < YAML < env vars < CLI flags."""
    values: dict[str, Any] = {}

    # 1. YAML file
    if config_path is not None:
        with open(config_path) as fh:
            yaml_data = yaml.safe_load(fh)
        if isinstance(yaml_data, dict):
            values.update(yaml_data)

    # 2. Environment variables
    for env_key, (field_name, field_type) in _ENV_MAP.items():
        env_val = os.environ.get(env_key)
        if env_val is not None:
            values[field_name] = _coerce(env_val, field_type)

    # 3. CLI overrides (Click passes None for unset flags — skip those)
    for key, val in cli_overrides.items():
        if val is not None:
            values[key] = val

    return Config(**values)
