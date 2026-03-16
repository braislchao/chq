"""CLI entry point for chq."""

import logging
import sys

import click

from chq.config import CHECKS, load_config
from chq.runner import run


@click.command()
@click.option("--host", envvar="CHQ_HOST", default=None, help="ClickHouse host.")
@click.option("--port", envvar="CHQ_PORT", default=None, type=int, help="ClickHouse port (default: 8443).")
@click.option("--user", envvar="CHQ_USER", default=None, help="ClickHouse user (default: default).")
@click.option("--password", envvar="CHQ_PASSWORD", default=None, help="ClickHouse password.")
@click.option("--secure/--no-secure", envvar="CHQ_SECURE", default=None, help="Use TLS (default: true).")
@click.option("--table", envvar="CHQ_TABLE", default=None, help="Source table (default: system.query_log).")
@click.option("--config", "config_path", default=None, type=click.Path(exists=True), help="Path to YAML config file.")
@click.option(
    "--format", "fmt", envvar="CHQ_FORMAT", default=None,
    type=click.Choice(["terminal", "slack", "json", "csv"]),
    help="Output format (default: terminal).",
)
@click.option("--slack-webhook", envvar="CHQ_SLACK_WEBHOOK", default=None, help="Slack webhook URL.")
@click.option("-o", "--output", "output_path", default=None, help="Output file path (for json/csv).")
@click.option("--only", default=None, help="Comma-separated categories to run (e.g., top_n,anti_patterns).")
@click.option("--lookback-days", envvar="CHQ_LOOKBACK_DAYS", default=None, type=int, help="Analysis window in days (default: 7).")
@click.option("--top-n", envvar="CHQ_TOP_N", default=None, type=int, help="Number of results per check (default: 10).")
@click.option("--list-checks", is_flag=True, help="List available checks and exit.")
@click.option("--show-sql", is_flag=True, help="Print the SQL for each check instead of running it.")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose logging.")
def main(
    host, port, user, password, secure, table, config_path, fmt, slack_webhook,
    output_path, only, lookback_days, top_n, list_checks, show_sql, verbose,
):
    """chq — ClickHouse query performance analyzer.

    Analyzes system.query_log to find expensive queries, anti-patterns,
    regressions, and cost attribution. Works with any ClickHouse instance
    (Cloud, self-hosted, or on-prem).
    """
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    if list_checks:
        _print_checks()
        return

    # Build overrides dict from CLI flags (None values are skipped by load_config)
    cli_overrides = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "secure": secure,
        "table": table,
        "format": fmt,
        "slack_webhook": slack_webhook,
        "output_path": output_path,
        "lookback_days": lookback_days,
        "top_n": top_n,
    }

    if only is not None:
        cli_overrides["only_categories"] = [c.strip() for c in only.split(",")]

    config = load_config(config_path=config_path, **cli_overrides)

    if show_sql:
        from chq.executor import load_queries
        for q in load_queries(config):
            click.echo(f"-- {q.category}/{q.name}")
            click.echo(q.sql)
            click.echo()
        return

    if not config.host:
        click.echo("Error: ClickHouse host is required. Use --host, CHQ_HOST env var, or a config file.", err=True)
        sys.exit(1)

    try:
        run(config)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def _print_checks():
    """Print all available checks with descriptions."""
    click.echo("Available checks:\n")
    current_category = None
    for (category, name), description in sorted(CHECKS.items()):
        if category != current_category:
            current_category = category
            click.echo(f"  {category}/")
        click.echo(f"    {name:40s} {description}")
    click.echo()


if __name__ == "__main__":
    main()
