"""Tests for output formatters."""

import json
from pathlib import Path

from chq.config import Config
from chq.executor import QueryResult
from chq.outputs.json_out import output_json
from chq.outputs.slack import format_slack

FIXTURES = Path(__file__).parent / "fixtures" / "sample_results.json"


def _load_fixtures() -> list[QueryResult]:
    """Load sample results from the fixture file."""
    with open(FIXTURES) as fh:
        raw = json.load(fh)
    return [
        QueryResult(
            category=r["category"],
            name=r["name"],
            columns=r["columns"],
            rows=[tuple(row) for row in r["rows"]],
        )
        for r in raw
    ]


class TestSlackFormatter:
    def test_produces_blocks(self):
        results = _load_fixtures()
        config = Config(lookback_days=7)
        payloads = format_slack(results, config)
        assert len(payloads) >= 1
        for payload in payloads:
            assert "blocks" in payload
            assert len(payload["blocks"]) <= 50

    def test_skips_empty_results(self):
        """Checks with 0 rows should not generate row blocks."""
        results = _load_fixtures()
        config = Config(lookback_days=7)
        payloads = format_slack(results, config)
        all_text = json.dumps(payloads)
        # new_expensive_patterns has 0 rows in fixtures — its name should
        # still not appear as a section header
        assert "New Expensive Patterns" not in all_text

    def test_blocks_are_valid_json(self):
        results = _load_fixtures()
        config = Config(lookback_days=7)
        payloads = format_slack(results, config)
        # Should be serializable to JSON without error
        serialized = json.dumps(payloads)
        assert len(serialized) > 0


class TestJsonOutput:
    def test_json_to_stdout(self, capsys):
        results = _load_fixtures()
        config = Config(lookback_days=7)
        output_json(results, config)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert "generated_at" in data
        assert "checks" in data
        assert "top_n/by_memory" in data["checks"]
        assert len(data["checks"]["top_n/by_memory"]["rows"]) == 2

    def test_json_to_file(self, tmp_path):
        results = _load_fixtures()
        out_file = str(tmp_path / "report.json")
        config = Config(lookback_days=7, output_path=out_file)
        output_json(results, config)
        with open(out_file) as fh:
            data = json.load(fh)
        assert "checks" in data


class TestCsvOutput:
    def test_csv_to_stdout(self, capsys):
        from chq.outputs.csv_out import output_csv

        results = _load_fixtures()
        config = Config(lookback_days=7)
        output_csv(results, config)
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        # Should have at least comment + header + data rows
        assert any(line.startswith("# top_n/by_memory") for line in lines)
