"""Tests for table-to-owner annotation."""

import json

import pytest

from chq.executor import QueryResult
from chq.owners import UNOWNED, OwnersMap, annotate, load_owners_map

PATTERNS = {
    "*": {"team": "Catch All"},
    "*state_history_*": {"team": "Platform", "contact": "#platform"},
    "*state_history_employees*": {"team": "Contracts", "contact": "#contracts"},
}


class TestOwnersMap:
    def test_longest_pattern_wins(self):
        """Specificity must not depend on the order of the file."""
        owners = OwnersMap(PATTERNS)
        assert owners.team_of("db.state_history_employees") == "Contracts"
        assert owners.team_of("db.state_history_contracts") == "Platform"
        assert owners.team_of("db.something_else") == "Catch All"

    def test_backticks_are_ignored(self):
        """ClickHouse quotes names containing dots; the quotes are not part of the name."""
        owners = OwnersMap({"*production.mv_state_history_employees*": {"team": "Contracts"}})
        assert owners.team_of("db.`production.mv_state_history_employees`") == "Contracts"

    def test_unmatched_is_reported_not_dropped(self):
        owners = OwnersMap({"*nope*": {"team": "Nobody"}})
        assert owners.team_of("db.orphan") == UNOWNED
        assert owners.contact_of("db.orphan") == ""

    def test_explicit_default(self):
        owners = OwnersMap({}, default={"team": "Data Platform", "contact": "#data"})
        assert owners.team_of("db.anything") == "Data Platform"
        assert owners.contact_of("db.anything") == "#data"


class TestLoadOwnersMap:
    def test_loads_yaml(self, tmp_path):
        p = tmp_path / "owners.yml"
        p.write_text('owners:\n  "*foo*": {team: Foo}\ndefault: {team: Nobody}\n')
        owners = load_owners_map(str(p))
        assert owners.team_of("db.foo_table") == "Foo"
        assert owners.team_of("db.bar") == "Nobody"

    def test_loads_json(self, tmp_path):
        p = tmp_path / "owners.json"
        p.write_text(json.dumps({"owners": {"*foo*": {"team": "Foo"}}}))
        assert load_owners_map(str(p)).team_of("db.foo_table") == "Foo"

    def test_rejects_non_mapping(self, tmp_path):
        p = tmp_path / "owners.yml"
        p.write_text("- not\n- a\n- mapping\n")
        with pytest.raises(ValueError, match="must be a mapping"):
            load_owners_map(str(p))

    def test_rejects_bad_owners_key(self, tmp_path):
        p = tmp_path / "owners.yml"
        p.write_text("owners:\n  - listed\n")
        with pytest.raises(ValueError, match="'owners' must be a mapping"):
            load_owners_map(str(p))


class TestAnnotate:
    def _owners(self):
        return OwnersMap({"*hot*": {"team": "Team Hot", "contact": "#hot"}})

    def test_appends_owner_columns_for_table_rows(self):
        qr = QueryResult(
            category="cost_attribution",
            name="by_table",
            columns=["table", "cpu_seconds"],
            rows=[("db.hot_table", 10), ("db.cold_table", 5)],
        )
        out = annotate([qr], self._owners())[0]
        assert out.columns == ["table", "cpu_seconds", "owner", "owner_contact"]
        assert out.rows[0] == ("db.hot_table", 10, "Team Hot", "#hot")
        assert out.rows[1] == ("db.cold_table", 5, UNOWNED, "")

    def test_uses_mv_column_when_present(self):
        qr = QueryResult(category="materialized_views", name="refresh_cost", columns=["mv", "cpu_seconds"], rows=[("db.hot_view", 1)])
        assert annotate([qr], self._owners())[0].rows[0][-2] == "Team Hot"

    def test_qualifies_schema_audit_with_database(self):
        """schema_audit splits the name, so the pattern still needs db.table."""
        qr = QueryResult(
            category="schema_audit",
            name="missing_company_id_sorting_key",
            columns=["database", "table_name", "issue"],
            rows=[("mydb", "hot_table", "no company_id")],
        )
        owners = OwnersMap({"mydb.hot*": {"team": "Team Hot"}})
        assert annotate([qr], owners)[0].rows[0][-2] == "Team Hot"

    def test_leaves_results_without_a_subject_untouched(self):
        qr = QueryResult(category="top_n", name="by_memory", columns=["primary_user", "peak_memory"], rows=[("bob", "1 GiB")])
        out = annotate([qr], self._owners())[0]
        assert out.columns == ["primary_user", "peak_memory"]
        assert out.rows == [("bob", "1 GiB")]
