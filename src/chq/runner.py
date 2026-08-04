"""Orchestrator: load queries, execute, format, output."""

import importlib
import logging
from dataclasses import replace
from pathlib import Path

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


def _dispatch(fmt: str, results, config: Config) -> None:
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


def run(config: Config) -> int:
    """Run all enabled checks and output results in the configured format.

    Returns the number of checks that failed. The report is still produced for
    the checks that succeeded, so the caller can emit output and exit non-zero.
    """
    execution = execute_queries(config)
    results, failures = execution.results, execution.failures

    if not results:
        logger.warning("No query results to report.")
        _log_failures(failures)
        return len(failures)

    total_rows = sum(len(r.rows) for r in results)
    logger.info("Executed %d checks, %d total result rows.", len(results), total_rows)

    fmt = config.format

    if fmt == "all":
        _run_all(results, config)
    elif fmt not in OUTPUT_DISPATCHERS:
        raise ValueError(f"Unknown output format: {fmt!r}. Choose from: {', '.join(OUTPUT_DISPATCHERS)}, all")
    else:
        _dispatch(fmt, results, config)

    _log_failures(failures)
    return len(failures)


def _log_failures(failures) -> None:
    """Report failed checks, so a partial report is never mistaken for a clean one."""
    if not failures:
        return
    logger.error(
        "%d check(s) failed and are missing from this report: %s",
        len(failures),
        ", ".join(f"{f.category}/{f.name}" for f in failures),
    )


def _run_all(results, config: Config) -> None:
    """Output all formats in one pass: terminal to stdout + html/json/csv to files."""
    base = Path(config.output_path).stem if config.output_path else "report"

    file_formats = {"html": f"{base}.html", "json": f"{base}.json", "csv": f"{base}.csv"}

    _dispatch("terminal", results, config)

    for fmt, path in file_formats.items():
        _dispatch(fmt, results, replace(config, output_path=path))
        logger.info("Wrote %s → %s", fmt.upper(), path)
