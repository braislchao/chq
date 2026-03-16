"""Slack Block Kit formatting and webhook posting for chq reports."""

from __future__ import annotations

import json
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

CATEGORY_EMOJI = {
    "top_n": ":fire:",
    "anomalies": ":chart_with_upwards_trend:",
    "cost_attribution": ":bar_chart:",
    "anti_patterns": ":warning:",
    "cluster_health": ":gear:",
}

CATEGORY_TITLES = {
    "top_n": "Top N \u2014 Most Expensive Queries",
    "anomalies": "Anomalies \u2014 Week-over-Week Changes",
    "cost_attribution": "Cost Attribution",
    "anti_patterns": "Anti-Patterns Detected",
    "cluster_health": "Cluster Health \u2014 Merges, Parts & Ingestion",
}

MAX_BLOCKS_PER_MESSAGE = 50
SAFE_BLOCK_LIMIT = 45
MAX_ROWS_PER_CHECK = 5


def _humanize_name(name: str) -> str:
    return name.replace("_", " ").title()


def _truncate(value: object, max_len: int = 100) -> str:
    text = str(value)
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def _col_value(row: tuple, columns: list[str], col_name: str) -> str | None:
    """Safely extract a column value from a row by column name."""
    try:
        idx = columns.index(col_name)
        return str(row[idx])
    except (ValueError, IndexError):
        return None


def _format_row(row: tuple, columns: list[str], category: str) -> str:
    """Format a single result row as a compact Slack mrkdwn string.

    Uses the actual column names produced by the SQL queries to build a
    readable one-liner per result row.
    """
    parts: list[str] = []
    query = _col_value(row, columns, "query_text")
    if query:
        parts.append(f"`{_truncate(query, 100)}`")

    # Pick the most relevant metric columns by category
    if category == "top_n":
        for col in ("peak_memory_readable", "total_read_readable",
                     "p95_duration_ms", "weighted_cost_readable"):
            val = _col_value(row, columns, col)
            if val is not None:
                label = col.replace("_readable", "").replace("_", " ").title()
                parts.append(f"*{label}:* {val}")
                break
        executions = _col_value(row, columns, "executions")
        if executions:
            parts.append(f"*Runs:* {executions}")

    elif category == "anomalies":
        for col in ("duration_change_pct", "bytes_change_pct"):
            val = _col_value(row, columns, col)
            if val is not None:
                parts.append(f"*Change:* {val}%")
                break
        prev = _col_value(row, columns, "prev_p95_ms")
        curr = _col_value(row, columns, "curr_p95_ms")
        if prev and curr:
            parts.append(f"*p95:* {prev}ms -> {curr}ms")

    elif category == "cost_attribution":
        user = _col_value(row, columns, "user")
        if user:
            parts = [f"*{user}*"]
        for col in ("total_read_readable", "total_hours", "total_queries", "error_rate_pct"):
            val = _col_value(row, columns, col)
            if val is not None:
                label = col.replace("_pct", " %").replace("_", " ").title()
                parts.append(f"*{label}:* {val}")

    elif category == "anti_patterns":
        for col in ("scan_ratio", "avg_per_hour", "avg_result_rows",
                     "avg_written_rows", "avg_selected_parts", "executions"):
            val = _col_value(row, columns, col)
            if val is not None:
                label = col.replace("_", " ").title()
                parts.append(f"*{label}:* {val}")
                break
        executions = _col_value(row, columns, "executions")
        if executions and not any("Executions" in p for p in parts):
            parts.append(f"*Runs:* {executions}")

    elif category == "cluster_health":
        # Show database.table identification
        db = _col_value(row, columns, "database")
        tbl = _col_value(row, columns, "table")
        if db and tbl:
            parts.append(f"`{db}.{tbl}`")
        # Show the status/class column (varies by check)
        for col in ("batch_size_class", "merge_pressure_class",
                     "merge_duration_class", "partition_status", "engine"):
            val = _col_value(row, columns, col)
            if val is not None:
                parts.append(f"*{val}*")
                break
        # Show key metrics
        for col in ("added_parts", "total_merges", "active_parts",
                     "inserts", "small_parts_pct", "avg_rows_per_part",
                     "p90_merge_duration_s", "avg_part_size_mb"):
            val = _col_value(row, columns, col)
            if val is not None:
                label = col.replace("_", " ").title()
                parts.append(f"*{label}:* {val}")
                if len(parts) >= 5:
                    break

    # Always append user at the end (except cost_attribution which leads with it)
    if category != "cost_attribution":
        user = _col_value(row, columns, "primary_user")
        if user:
            parts.append(f"_({user})_")

    # Fallback: if nothing matched, show all columns
    if not parts:
        for col, val in zip(columns, row):
            parts.append(f"*{col}:* {_truncate(str(val), 80)}")

    return " | ".join(parts)


def _build_category_blocks(category: str, checks: list) -> list[dict]:
    """Build Slack blocks for a single category and its checks."""
    emoji = CATEGORY_EMOJI.get(category, ":clipboard:")
    title = CATEGORY_TITLES.get(category, category.replace("_", " ").title())

    blocks: list[dict] = [
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{emoji} *{title}*",
            },
        },
    ]

    for qr in checks:
        if not qr.rows:
            continue

        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{_humanize_name(qr.name)}*",
                },
            }
        )

        display_rows = qr.rows[:MAX_ROWS_PER_CHECK]
        for row in display_rows:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": _format_row(row, qr.columns, qr.category),
                    },
                }
            )

        remaining = len(qr.rows) - MAX_ROWS_PER_CHECK
        if remaining > 0:
            blocks.append(
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"and {remaining} more...",
                        }
                    ],
                }
            )

    return blocks


def format_slack(results: list, config) -> list[dict]:
    """Build Slack Block Kit payloads from *results*.

    Returns a list of message payloads (dicts with a ``"blocks"`` key).  If the
    total block count would exceed the safe limit the results are split into
    one message per category.
    """
    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(days=config.lookback_days)
    date_range = f"{start:%Y-%m-%d} to {now:%Y-%m-%d}"

    header_blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "ClickHouse Performance Report",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Date range: *{date_range}* (lookback: {config.lookback_days} days)",
                }
            ],
        },
    ]

    # Group by category, preserving first-seen order.
    grouped: dict[str, list] = {}
    for qr in results:
        grouped.setdefault(qr.category, []).append(qr)

    # Build blocks per category.
    category_block_groups: list[list[dict]] = []
    for category, checks in grouped.items():
        cat_blocks = _build_category_blocks(category, checks)
        if cat_blocks:
            category_block_groups.append(cat_blocks)

    # Calculate total block count to decide on splitting.
    total_blocks = len(header_blocks) + sum(len(b) for b in category_block_groups)

    if total_blocks <= SAFE_BLOCK_LIMIT:
        all_blocks = list(header_blocks)
        for group in category_block_groups:
            all_blocks.extend(group)
        return [{"blocks": all_blocks}]

    # Split: one message per category, header only in the first.
    payloads: list[dict] = []
    for i, group in enumerate(category_block_groups):
        if i == 0:
            blocks = list(header_blocks) + group
        else:
            blocks = list(group)
        payloads.append({"blocks": blocks})

    return payloads


def send_slack(payloads: list[dict], webhook_url: str) -> None:
    """Post each payload to the Slack webhook.

    Uses :mod:`urllib.request` (stdlib) so there is no dependency on
    ``requests``.  Raises :class:`RuntimeError` on non-200 responses.
    """
    for payload in payloads:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:  # noqa: S310
            if resp.status != 200:
                body = resp.read().decode("utf-8", errors="replace")
                raise RuntimeError(
                    f"Slack webhook returned status {resp.status}: {body}"
                )
