"""Guards the Slack formatter against column-name drift.

The formatter looks columns up by name and silently omits anything it cannot
find, so renaming a column in a .sql file degrades the Slack row to bare query
text rather than failing. These tests pin the contract in both directions.
"""

import json
from pathlib import Path

import pytest

from chq.config import CHECKS
from chq.outputs.slack import _format_row

FIXTURE = Path(__file__).parent / "fixtures" / "check_columns.json"

with open(FIXTURE) as fh:
    _CHECK_COLUMNS: dict[str, dict] = json.load(fh)["checks"]


def _sentinel_row(columns: list[str]) -> tuple:
    """Build a row whose every cell is a value unique to its column name."""
    return tuple(f"SENTINEL-{col}" for col in columns)


@pytest.mark.parametrize("check", sorted(_CHECK_COLUMNS))
def test_headline_column_is_rendered(check: str):
    """Every check must surface the one column that is its point.

    Without this, a check can render as nothing but the query text and still
    look healthy, which is how the *_readable columns went unnoticed.
    """
    category = check.split("/")[0]
    spec = _CHECK_COLUMNS[check]
    columns = spec["columns"]
    headline = spec["headline"]

    assert headline in columns, f"{check}: headline {headline!r} is not one of its columns"

    rendered = _format_row(_sentinel_row(columns), columns, category)

    assert f"SENTINEL-{headline}" in rendered, (
        f"{check}: the Slack formatter does not render its headline column "
        f"{headline!r}. Either the column was renamed in the .sql file without "
        f"updating outputs/slack.py, or this check has no branch in _format_row."
    )


@pytest.mark.parametrize("check", sorted(_CHECK_COLUMNS))
def test_row_is_not_bare_query_text(check: str):
    """A rendered row must carry at least one column beyond the query text."""
    category = check.split("/")[0]
    columns = _CHECK_COLUMNS[check]["columns"]

    rendered = _format_row(_sentinel_row(columns), columns, category)
    surfaced = {col for col in columns if f"SENTINEL-{col}" in rendered}

    assert surfaced - {"query_text"}, f"{check}: row rendered with nothing but query text"


def test_fixture_covers_every_registered_check():
    """The fixture must track every check in the registry, and no phantom ones."""
    registered = {f"{cat}/{name}" for cat, name in CHECKS}
    fixtured = set(_CHECK_COLUMNS)

    assert not registered - fixtured, (
        f"checks missing from {FIXTURE.name}: {sorted(registered - fixtured)}. "
        "Add their columns so the Slack output stays covered."
    )
    assert not fixtured - registered, f"{FIXTURE.name} lists unregistered checks: {sorted(fixtured - registered)}"
