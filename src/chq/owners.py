"""Optional table-to-owner annotation.

chq can tell you which table or materialized view is expensive, but not who is
responsible for it, because that mapping lives in whatever repository defines the
schema. This module takes a caller-supplied map and adds the owner to every row
that names a table or a view, so a report can be handed to a team rather than
interpreted by whoever ran it.

Map format (JSON or YAML)::

    owners:
      "*.analytics_orders_daily_*": {team: "Orders", contact: "#orders-eng"}
      "*mv_state_history_users*": {team: "Accounts", contact: "#accounts-eng"}
    default: {team: "UNOWNED"}

Patterns are shell globs matched against the value of the row's ``table`` or
``mv`` column. The longest matching pattern wins, so the result does not depend
on the order of the file. Rows that match nothing get the ``default`` entry, or
``UNOWNED`` if no default is given: unmatched tables are reported rather than
dropped, because a silently missing owner looks the same as a table nobody uses.
"""

from __future__ import annotations

import json
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

import yaml

from chq.executor import QueryResult

# Columns that identify the thing being owned, in priority order.
SUBJECT_COLUMNS = ("mv", "table", "table_name")

UNOWNED = "UNOWNED"


class OwnersMap:
    """Resolves a table or view name to an owning team."""

    def __init__(self, patterns: dict[str, dict[str, Any]], default: dict[str, Any] | None = None):
        # Longest pattern first so the most specific rule wins regardless of file order.
        self._patterns = sorted(patterns.items(), key=lambda kv: len(kv[0]), reverse=True)
        self._default = default or {"team": UNOWNED}

    def resolve(self, subject: str) -> dict[str, Any]:
        """Return the owner entry for *subject*, never None."""
        # ClickHouse quotes names containing dots, and the quotes are not part of
        # the identifier anyone writes a pattern against.
        candidates = (subject, subject.replace("`", ""))
        for pattern, entry in self._patterns:
            if any(fnmatch(c, pattern) for c in candidates):
                return entry
        return self._default

    def team_of(self, subject: str) -> str:
        return str(self.resolve(subject).get("team", UNOWNED))

    def contact_of(self, subject: str) -> str:
        return str(self.resolve(subject).get("contact", ""))


def load_owners_map(path: str) -> OwnersMap:
    """Load an owners map from a JSON or YAML file."""
    text = Path(path).read_text(encoding="utf-8")
    # yaml.safe_load parses JSON too, so one call covers both formats.
    data = yaml.safe_load(text) if not path.endswith(".json") else json.loads(text)
    if not isinstance(data, dict):
        raise ValueError(f"{path}: owners map must be a mapping, got {type(data).__name__}")

    patterns = data.get("owners") or {}
    if not isinstance(patterns, dict):
        raise ValueError(f"{path}: 'owners' must be a mapping of glob -> owner")

    return OwnersMap(patterns=patterns, default=data.get("default"))


def annotate(results: list[QueryResult], owners: OwnersMap) -> list[QueryResult]:
    """Append owner columns to every result that names a table or a view.

    Results without a subject column are returned untouched, so this is safe to
    apply to the whole run.
    """
    annotated: list[QueryResult] = []
    for qr in results:
        subject_col = next((c for c in SUBJECT_COLUMNS if c in qr.columns), None)
        if subject_col is None:
            annotated.append(qr)
            continue

        idx = qr.columns.index(subject_col)
        # schema_audit splits the name across database and table_name, so the
        # pattern needs the qualified name to match the same way as elsewhere.
        db_idx = qr.columns.index("database") if subject_col == "table_name" and "database" in qr.columns else None

        rows = []
        for row in qr.rows:
            subject = str(row[idx])
            if db_idx is not None:
                subject = f"{row[db_idx]}.{subject}"
            entry = owners.resolve(subject)
            rows.append((*row, str(entry.get("team", UNOWNED)), str(entry.get("contact", ""))))

        annotated.append(
            QueryResult(
                category=qr.category,
                name=qr.name,
                columns=[*qr.columns, "owner", "owner_contact"],
                rows=rows,
            )
        )
    return annotated
