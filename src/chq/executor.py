"""SQL loading and ClickHouse execution for chq."""

from __future__ import annotations

import importlib.resources
import logging
import re
from collections import namedtuple

import clickhouse_connect

from chq.config import Config

log = logging.getLogger(__name__)

SQLQuery = namedtuple("SQLQuery", ["category", "name", "sql"])
QueryResult = namedtuple("QueryResult", ["category", "name", "columns", "rows"])

# Results plus the checks that raised. Failures are collected rather than raised
# immediately so one broken check still yields a report for the other 24, but the
# caller can exit non-zero instead of presenting a silently partial report as
# complete.
Execution = namedtuple("Execution", ["results", "failures"])
CheckFailure = namedtuple("CheckFailure", ["category", "name", "error"])


def load_queries(config: Config) -> list[SQLQuery]:
    """Discover and template-render all SQL files bundled in the package.

    SQL files live under ``chq/sql/{category}/{name}.sql``.  Template
    placeholders such as ``{lookback_days}`` are substituted using
    :pyattr:`Config.sql_params`.
    """
    sql_root = importlib.resources.files("chq") / "sql"
    params = config.sql_params
    queries: list[SQLQuery] = []

    for category_dir in sorted(sql_root.iterdir()):
        if not category_dir.is_dir():
            continue

        category = category_dir.name

        if config.only_categories is not None and category not in config.only_categories:
            continue

        for sql_file in sorted(category_dir.iterdir()):
            if not sql_file.name.endswith(".sql"):
                continue

            name = sql_file.name.removesuffix(".sql")
            raw_sql = sql_file.read_text(encoding="utf-8")
            rendered_sql = _substitute(raw_sql, params)
            queries.append(SQLQuery(category=category, name=name, sql=rendered_sql))

    return queries


def execute_queries(config: Config) -> Execution:
    """Connect to ClickHouse, run every discovered query, and return results.

    A check that raises is recorded in ``Execution.failures`` and does not stop
    the run, so a single broken check still produces a report for the rest. The
    caller is expected to surface those failures, because a report that quietly
    omits a check looks the same as one where the check found nothing.
    """
    client = clickhouse_connect.get_client(
        host=config.host,
        port=config.port,
        username=config.user,
        password=config.password,
        secure=config.secure,
    )

    results: list[QueryResult] = []
    failures: list[CheckFailure] = []
    try:
        for q in load_queries(config):
            try:
                result = client.query(q.sql)
                results.append(
                    QueryResult(
                        category=q.category,
                        name=q.name,
                        columns=result.column_names,
                        rows=result.result_rows,
                    )
                )
            except Exception:
                # Format the current exception without a traceback for a concise
                # log line; the full traceback is still available at DEBUG level
                # via the root logger if needed.
                error = _current_exc_oneline()
                log.warning("Query %s/%s failed: %s", q.category, q.name, error)
                failures.append(CheckFailure(category=q.category, name=q.name, error=error))
    finally:
        client.close()

    return Execution(results=results, failures=failures)


def _substitute(sql: str, params: dict) -> str:
    """Replace {param} placeholders with values from *params*.

    Only known parameter names are replaced. Literal braces (e.g., in
    JSONExtract or Map access like ``col['key']``) are left untouched,
    unlike str.format_map which would raise on them.
    """

    def _replace(match: re.Match) -> str:
        key = match.group(1)
        if key in params:
            return str(params[key])
        return match.group(0)  # leave unknown placeholders as-is

    return re.sub(r"\{(\w+)\}", _replace, sql)


def _current_exc_oneline() -> str:
    """Return a single-line representation of the current exception."""
    import sys

    exc = sys.exc_info()[1]
    if exc is None:
        return "<no exception>"
    return f"{type(exc).__name__}: {exc}"
