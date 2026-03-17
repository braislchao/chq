"""Rich terminal output for chq reports."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from rich.console import Console
from rich.table import Table

if TYPE_CHECKING:
    pass

CATEGORY_TITLES = {
    "top_n": "Top N \u2014 Most Expensive Queries",
    "anomalies": "Anomalies \u2014 Week-over-Week Changes",
    "cost_attribution": "Cost Attribution",
    "anti_patterns": "Anti-Patterns Detected",
    "cluster_health": "Cluster Health \u2014 Merges, Parts & Ingestion",
}

MAX_CELL_WIDTH = 80


def _humanize_name(name: str) -> str:
    """Convert a snake_case check name to a human-readable title."""
    return name.replace("_", " ").title()


def _truncate(value: object, max_len: int = MAX_CELL_WIDTH) -> str:
    """Return string representation of *value*, truncated if necessary."""
    text = str(value)
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def output_terminal(results: list, config) -> None:
    """Print a formatted performance report to the terminal using Rich."""
    console = Console()

    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(days=config.lookback_days)
    date_range = f"{start:%Y-%m-%d} to {now:%Y-%m-%d}"

    console.print()
    console.rule("[bold blue]ClickHouse Query Performance Report[/bold blue]")
    console.print(f"[dim]Date range: {date_range}[/dim]", justify="center")
    console.print()

    # Group results by category, preserving order of first appearance.
    grouped: dict[str, list] = {}
    for qr in results:
        grouped.setdefault(qr.category, []).append(qr)

    for category, checks in grouped.items():
        title = CATEGORY_TITLES.get(category, category.replace("_", " ").title())
        console.rule(f"[bold green]{title}[/bold green]")
        console.print()

        for qr in checks:
            human_name = _humanize_name(qr.name)
            console.print(f"  [bold]{human_name}[/bold]")

            if not qr.rows:
                console.print("  [dim]No results[/dim]")
                console.print()
                continue

            table = Table(show_header=True, header_style="bold cyan", padding=(0, 1))
            for col in qr.columns:
                table.add_column(col)

            for row in qr.rows:
                table.add_row(*[_truncate(v) for v in row])

            console.print(table)
            console.print()

    console.rule("[dim]End of report[/dim]")
    console.print()
