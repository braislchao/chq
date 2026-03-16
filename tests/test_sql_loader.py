"""Tests for SQL loading and template substitution."""

import pytest

from chq.config import CHECKS, Config
from chq.executor import load_queries


def test_load_all_queries():
    """All SQL files load without error and all placeholders are substituted."""
    config = Config()
    queries = load_queries(config)
    assert len(queries) == len(CHECKS), (
        f"Expected {len(CHECKS)} queries, got {len(queries)}"
    )
    for q in queries:
        # No unsubstituted placeholders should remain
        assert "{" not in q.sql, (
            f"{q.category}/{q.name} has unsubstituted placeholders"
        )


def test_load_filtered_categories():
    """Only queries from requested categories are loaded."""
    config = Config(only_categories=["top_n"])
    queries = load_queries(config)
    categories = {q.category for q in queries}
    assert categories == {"top_n"}
    assert len(queries) == 4


def test_all_checks_have_sql():
    """Every entry in CHECKS corresponds to a loadable SQL file."""
    config = Config()
    queries = load_queries(config)
    loaded = {(q.category, q.name) for q in queries}
    for key in CHECKS:
        assert key in loaded, f"Check {key} is in CHECKS but no SQL file was loaded"


def test_sql_params_substitution():
    """Custom threshold values are properly substituted into SQL."""
    config = Config(lookback_days=30, top_n=20)
    queries = load_queries(config)
    # Find a top_n query and verify the values appear
    top_query = next(q for q in queries if q.category == "top_n")
    assert "today() - 30" in top_query.sql
    assert "LIMIT 20" in top_query.sql
