"""JSON output formatter for chq reports."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone


def output_json(results: list, config) -> None:
    """Serialize *results* to JSON and write to file or stdout.

    The output is a structured dict with metadata and all check results.
    Each row is represented as a dict mapping column names to values.
    """
    now = datetime.now(tz=timezone.utc)

    checks: dict[str, dict] = {}
    for qr in results:
        key = f"{qr.category}/{qr.name}"
        checks[key] = {
            "category": qr.category,
            "name": qr.name,
            "columns": list(qr.columns),
            "rows": [dict(zip(qr.columns, row)) for row in qr.rows],
        }

    report = {
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "lookback_days": config.lookback_days,
        "checks": checks,
    }

    text = json.dumps(report, indent=2, default=str)

    output_path = getattr(config, "output_path", None)
    if output_path:
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.write("\n")
    else:
        sys.stdout.write(text)
        sys.stdout.write("\n")
