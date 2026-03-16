# chq — ClickHouse Query Performance Analyzer

A CLI tool that analyzes `system.query_log` to find expensive queries, anti-patterns, regressions, and cost attribution. Works with **any ClickHouse instance** — Cloud, self-hosted, or on-prem.

## What it does

`chq` runs 13 analytical checks against your ClickHouse query log and reports the results. It helps you answer questions like:

- Which queries consume the most memory, scan the most data, or take the longest?
- Did any query pattern regress this week compared to last week?
- Which users or services are driving the most resource consumption?
- Are there queries using anti-patterns like `SELECT *`, missing partition filters, or unbounded result sets?

## Install

```bash
pip install .
```

Or for development:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Quick start

```bash
# Using CLI flags
chq --host your-clickhouse-host --user default --password yourpassword

# Using environment variables
export CHQ_HOST=your-clickhouse-host
export CHQ_USER=default
export CHQ_PASSWORD=yourpassword
chq

# Using a config file
cp config.yaml.example config.yaml
# Edit config.yaml with your connection details
chq --config config.yaml
```

### Self-hosted ClickHouse (no TLS)

```bash
chq --host localhost --port 8123 --no-secure --user default
```

### ClickHouse Cloud

```bash
chq --host abc123.clickhouse.cloud --port 8443 --user default --password yourpassword
```

## Output formats

```bash
# Rich terminal tables (default)
chq --format terminal

# Post to Slack
chq --format slack --slack-webhook https://hooks.slack.com/services/T.../B.../xxx

# JSON (to stdout or file)
chq --format json
chq --format json -o report.json

# CSV
chq --format csv -o report.csv
```

## Checks

`chq` runs 13 checks organized in 4 categories. List them with:

```bash
chq --list-checks
```

### Top N — Most Expensive Queries

| Check | What it finds |
|---|---|
| `by_memory` | Queries with the highest peak memory usage |
| `by_read_bytes` | Queries that scan the most data in aggregate |
| `by_duration` | Queries with the highest p95 execution time |
| `by_weighted_cost` | Queries with the highest frequency x average cost (catches "death by a thousand cuts") |

### Anomalies — Week-over-Week Changes

| Check | What it finds |
|---|---|
| `wow_duration_regressions` | Queries whose p95 duration regressed compared to last week |
| `new_expensive_patterns` | New query patterns that appeared this week and are already expensive |

### Cost Attribution

| Check | What it finds |
|---|---|
| `by_user` | Resource consumption breakdown per ClickHouse user (bytes scanned, compute hours, error rate) |

### Anti-Patterns

| Check | What it finds |
|---|---|
| `full_scans` | Queries scanning far more rows than they return (high read/result ratio) |
| `select_star_wide_tables` | Queries using `SELECT *` instead of projecting specific columns |
| `missing_partition_filter` | Queries scanning too many parts (likely missing a partition key filter) |
| `unbounded_results` | Queries returning large result sets without a `LIMIT` clause |
| `repeated_identical` | Same query running hundreds of times per hour (missing cache or polling loop) |
| `small_insert_batches` | INSERT queries writing very few rows per batch (causes part explosion) |

## Configuration

### CLI flags

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--host` | `CHQ_HOST` | — | ClickHouse host (**required**) |
| `--port` | `CHQ_PORT` | `8443` | ClickHouse native/HTTP port |
| `--user` | `CHQ_USER` | `default` | ClickHouse user |
| `--password` | `CHQ_PASSWORD` | — | ClickHouse password |
| `--secure/--no-secure` | `CHQ_SECURE` | `true` | Use TLS (`--no-secure` for local/self-hosted without TLS) |
| `--config` | — | — | Path to YAML config file |
| `--format` | `CHQ_FORMAT` | `terminal` | Output format: `terminal`, `slack`, `json`, `csv` |
| `--slack-webhook` | `CHQ_SLACK_WEBHOOK` | — | Slack webhook URL (required for `slack` format) |
| `-o, --output` | — | stdout | Output file path (for `json`/`csv`) |
| `--only` | — | all | Comma-separated categories: `top_n`, `anomalies`, `cost_attribution`, `anti_patterns` |
| `--lookback-days` | `CHQ_LOOKBACK_DAYS` | `7` | Analysis window in days |
| `--top-n` | `CHQ_TOP_N` | `10` | Number of results per check |
| `--list-checks` | — | — | List all available checks and exit |
| `-v, --verbose` | — | — | Enable debug logging |

### Config file

Create a `config.yaml` (see `config.yaml.example`):

```yaml
host: your-clickhouse-host
port: 8443
user: default
password: yourpassword
secure: true

format: terminal

lookback_days: 7
top_n: 10
```

Configuration priority (highest wins): **CLI flags > environment variables > config file > defaults**.

### Tuning thresholds

These thresholds control when a query is flagged. Defaults work well for most clusters, but you can tune them:

| Threshold | Default | What it controls |
|---|---|---|
| `lookback_days` | `7` | How far back to analyze |
| `top_n` | `10` | Number of results per check |
| `regression_threshold_pct` | `50` | Minimum % increase to flag a week-over-week regression |
| `min_executions` | `10` | Minimum executions for a query to be included in anomaly detection |
| `scan_ratio_threshold` | `1000` | read_rows/result_rows ratio to flag as a full scan |
| `max_result_rows` | `100000` | Result row count above which unbounded queries are flagged |
| `repeat_threshold` | `100` | Executions per hour to flag as repeated identical queries |
| `min_batch_size` | `1000` | INSERT batch size below which queries are flagged |
| `min_parts_threshold` | `20` | SelectedParts count above which queries are flagged for missing partition filter |

Set via CLI (`--lookback-days 14`), env var (`CHQ_LOOKBACK_DAYS=14`), or config file (`lookback_days: 14`).

## Permissions

`chq` only reads from `system.query_log`. The ClickHouse user needs:

```sql
GRANT SELECT ON system.query_log TO your_user;
```

No writes are performed against your ClickHouse instance.

## Scheduled reports with AWS Lambda

The `deploy/` directory contains a SAM template for running `chq` as a weekly Lambda function that posts to Slack.

### Setup

1. Store secrets in SSM Parameter Store:

```bash
aws ssm put-parameter --name /chq/clickhouse-host --value "your-host" --type String
aws ssm put-parameter --name /chq/clickhouse-user --value "readonly" --type String
aws ssm put-parameter --name /chq/clickhouse-password --value "xxx" --type SecureString
aws ssm put-parameter --name /chq/slack-webhook --value "https://hooks.slack.com/..." --type SecureString
```

2. Deploy:

```bash
cd deploy
sam build --use-container
sam deploy --guided
```

This creates a Lambda function triggered every Monday at 9:00 AM UTC via EventBridge.

## Running tests

```bash
source .venv/bin/activate
python -m pytest tests/ -v
```

## How it works

All analysis is done via SQL queries against `system.query_log`. The queries are bundled inside the package under `src/chq/sql/` and are organized by category. Each query:

- Filters on `event_date` (ClickHouse partition key) for efficient scans
- Filters on `is_initial_query = 1` to avoid double-counting distributed sub-queries
- Groups by `normalized_query_hash` to aggregate structurally identical queries regardless of literal values
- Returns human-readable columns alongside raw values

The SQL files use `{parameter}` placeholders (e.g., `{lookback_days}`, `{top_n}`) that are substituted at runtime with your configured thresholds.

## Project structure

```
src/chq/
  cli.py              CLI entry point (Click)
  config.py           Configuration loading (CLI flags, env vars, YAML, defaults)
  executor.py         SQL loading + ClickHouse execution
  runner.py           Orchestrator
  handler.py          AWS Lambda entry point
  sql/                SQL queries organized by category
    top_n/            4 checks — most expensive query patterns
    anomalies/        2 checks — week-over-week changes
    cost_attribution/ 1 check  — per-user resource breakdown
    anti_patterns/    6 checks — bad query patterns
  outputs/
    terminal.py       Rich terminal tables
    slack.py          Slack Block Kit formatting + webhook
    json_out.py       JSON export
    csv_out.py        CSV export
```
