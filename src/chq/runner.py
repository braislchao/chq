"""Orchestrator: load queries, execute, format, output."""

import logging

from chq.config import Config
from chq.executor import execute_queries

logger = logging.getLogger(__name__)

OUTPUT_DISPATCHERS = {
    "terminal": "chq.outputs.terminal",
    "slack": "chq.outputs.slack",
    "json": "chq.outputs.json_out",
    "csv": "chq.outputs.csv_out",
    "html": "chq.outputs.html",
}


def run(config: Config) -> None:
    """Run all enabled checks and output results in the configured format."""
    results = execute_queries(config)

    if not results:
        logger.warning("No query results to report.")
        return

    total_rows = sum(len(r.rows) for r in results)
    logger.info("Executed %d checks, %d total result rows.", len(results), total_rows)

    fmt = config.format
    if fmt not in OUTPUT_DISPATCHERS:
        raise ValueError(f"Unknown output format: {fmt!r}. Choose from: {', '.join(OUTPUT_DISPATCHERS)}")

    # Lazy import to avoid loading all formatters + their deps upfront
    import importlib

    mod = importlib.import_module(OUTPUT_DISPATCHERS[fmt])

    if fmt == "terminal":
        mod.output_terminal(results, config)
    elif fmt == "slack":
        payloads = mod.format_slack(results, config)
        if not config.slack_webhook:
            raise ValueError("--slack-webhook is required for slack output format.")
        mod.send_slack(payloads, config.slack_webhook)
        logger.info("Report posted to Slack.")
    elif fmt == "json":
        mod.output_json(results, config)
    elif fmt == "csv":
        mod.output_csv(results, config)
    elif fmt == "html":
        mod.output_html(results, config)
