# chq

Analyze `system.query_log` on any ClickHouse instance.
Finds expensive queries, anti-patterns, week-over-week regressions, and per-user cost attribution.

## Install

    pip install .

## Usage

    chq --host ch.example.com --password secret

    # or with env vars
    export CHQ_HOST=ch.example.com CHQ_PASSWORD=secret
    chq

    # self-hosted, no TLS
    chq --host localhost --port 8123 --no-secure

    # config file
    chq --config chq.yaml

Output goes to the terminal by default. Other formats:

    chq --format json                # stdout
    chq --format json -o report.json
    chq --format csv -o report.csv
    chq --format slack --slack-webhook https://hooks.slack.com/services/...

Run a subset of checks:

    chq --only top_n,anti_patterns
    chq --lookback-days 14 --top-n 20

List everything available:

    chq --list-checks

## Checks

13 checks in 4 categories, all driven by plain SQL against `system.query_log`.

| Category | Check | Description |
|---|---|---|
| top_n | `by_memory` | Peak memory usage |
| top_n | `by_read_bytes` | Total data scanned |
| top_n | `by_duration` | p95 execution time |
| top_n | `by_weighted_cost` | Frequency * avg bytes read |
| anomalies | `wow_duration_regressions` | p95 duration regressed vs. last week |
| anomalies | `new_expensive_patterns` | First seen this week, already expensive |
| cost_attribution | `by_user` | Bytes read, compute hours, error rate per user |
| anti_patterns | `full_scans` | Read/result row ratio above threshold |
| anti_patterns | `select_star_wide_tables` | SELECT * usage |
| anti_patterns | `missing_partition_filter` | Too many parts scanned |
| anti_patterns | `unbounded_results` | Large result sets without LIMIT |
| anti_patterns | `repeated_identical` | Same query >100 times/hour |
| anti_patterns | `small_insert_batches` | INSERT with <1000 rows per batch |

## Configuration

Settings are resolved in this order (last wins):

    defaults < config file < env vars < CLI flags

All env vars use the `CHQ_` prefix: `CHQ_HOST`, `CHQ_PORT`, `CHQ_USER`,
`CHQ_PASSWORD`, `CHQ_SECURE`, `CHQ_FORMAT`, `CHQ_SLACK_WEBHOOK`,
`CHQ_LOOKBACK_DAYS`, `CHQ_TOP_N`.

See `config.yaml.example` for the full set of options.

### Thresholds

Defaults are reasonable for most clusters. Override as needed:

    lookback_days              7       analysis window (days)
    top_n                     10       results per check
    regression_threshold_pct  50       min % increase to flag a regression
    min_executions            10       min runs for anomaly detection
    scan_ratio_threshold    1000       read_rows/result_rows to flag full scans
    max_result_rows       100000       flag unbounded results above this
    repeat_threshold         100       runs/hour to flag repeated queries
    min_batch_size          1000       INSERT rows below this are flagged
    min_parts_threshold       20       SelectedParts above this are flagged

## Permissions

Read-only. The ClickHouse user only needs:

    GRANT SELECT ON system.query_log TO your_user;

## Scheduled reports (Lambda)

A SAM template in `deploy/` runs `chq` weekly and posts to Slack.

    # store secrets
    aws ssm put-parameter --name /chq/clickhouse-host     --value "..." --type String
    aws ssm put-parameter --name /chq/clickhouse-password  --value "..." --type SecureString
    aws ssm put-parameter --name /chq/slack-webhook        --value "..." --type SecureString

    # deploy
    cd deploy && sam build --use-container && sam deploy --guided

Triggers every Monday at 09:00 UTC via EventBridge.

## Development

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -e '.[dev]'
    python -m pytest tests/ -v

## How it works

Each check is a standalone `.sql` file under `src/chq/sql/`.
At runtime, `{parameter}` placeholders are substituted with your thresholds,
then executed via `clickhouse-connect`. Results are piped to the selected
output formatter (terminal, slack, json, csv).

All queries filter on `event_date` (partition key) and `is_initial_query = 1`
to keep scans efficient and avoid double-counting distributed sub-queries.
Queries are grouped by `normalized_query_hash` so structurally identical
statements are aggregated regardless of literal values.

## License

MIT
