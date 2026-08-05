# chq

Analyze `system.query_log` on any ClickHouse instance.
Finds expensive queries, anti-patterns, week-over-week regressions, per-user cost attribution,
and cluster health diagnostics (merge pressure, partition fragmentation, insert patterns).

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
    chq --format html -o report.html   # self-contained, sortable tables
    chq --format slack --slack-webhook https://hooks.slack.com/services/...

Run a subset of checks:

    chq --only top_n,anti_patterns
    chq --lookback-days 14 --top-n 20

List everything available:

    chq --list-checks

## Checks

27 checks in 8 categories, driven by SQL against the query log (see
[Custom source table](#custom-source-table)), `system.part_log`, `system.parts`,
and other system tables.

| Category | Check | Description |
|---|---|---|
| top_n | `by_memory` | Peak memory usage |
| top_n | `by_read_bytes` | Total data scanned |
| top_n | `by_duration` | p95 execution time |
| top_n | `by_weighted_cost` | Frequency * avg bytes read |
| anomalies | `wow_duration_regressions` | p95 duration regressed vs. last week |
| anomalies | `new_expensive_patterns` | First seen this week, already expensive |
| cost_attribution | `by_user` | Bytes read, compute hours, error rate per user |
| cost_attribution | `by_table` | CPU and bytes read per table, the axis that maps to an owner |
| cost_attribution | `select_pct_by_db_and_engine` | SELECT distribution by database and storage engine |
| anti_patterns | `full_scans` | Read/result row ratio above threshold |
| anti_patterns | `select_star_wide_tables` | SELECT * usage |
| anti_patterns | `missing_partition_filter` | Too many parts scanned |
| anti_patterns | `unbounded_results` | Large result sets without LIMIT |
| anti_patterns | `repeated_identical` | Same query >100 times/hour |
| anti_patterns | `small_insert_batches` | INSERT with <1000 rows per batch |
| cluster_health | `insert_part_fragmentation` | Tables creating too many small parts from inserts |
| cluster_health | `insert_duration_by_engine` | Insert latency histogram by storage engine |
| cluster_health | `merge_pressure` | Merge duration, load, and pressure per table |
| cluster_health | `partition_health` | Partition fragmentation, merge backlog, oversized parts |
| materialized_views | `refresh_waste_summary` | One row: how much of this cluster's refresh work produced nothing |
| materialized_views | `refresh_cost` | Refreshable MV cost by CPU, with read amplification |
| materialized_views | `refresh_failures` | CPU and rows burnt by refreshes that wrote nothing |
| materialized_views | `refresh_wow` | MVs whose cost jumped, and newly expensive views |
| query_endpoints | `wow_usage` | Week-over-week call volume per Query API Endpoint |
| query_endpoints | `latency` | p50/p95/p99 duration per Query API Endpoint |
| query_endpoints | `data_scanned` | Average and total data scanned per endpoint call |
| schema_audit | `missing_company_id_sorting_key` | Operational tables whose sorting key lacks a tenant column |

## Configuration

Settings are resolved in this order (last wins):

    defaults < config file < env vars < CLI flags

All env vars use the `CHQ_` prefix: `CHQ_HOST`, `CHQ_PORT`, `CHQ_USER`,
`CHQ_PASSWORD`, `CHQ_SECURE`, `CHQ_FORMAT`, `CHQ_SLACK_WEBHOOK`,
`CHQ_LOOKBACK_DAYS`, `CHQ_TOP_N`, `CHQ_EXCLUDE_USERS` (comma-separated).

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

## Filtering noise

By default the ClickHouse Cloud service accounts are excluded from all
query-level checks: `observability-internal`, `monitoring-internal`,
`operator-internal`, `operator-cert-internal`, `prometheus-internal`,
`prometheus-exporter`, `management-internal`, `backups-internal`,
`availability-monitoring-internal`, `hyperdx-alert-internal` and
`hdx-alert-internal`. SQL console sessions are excluded by prefix, so
`sql-console:someone@example.com` is caught as well as bare `sql-console`. Bring
them all back with `--include-internal`.

`default` is **not** excluded by default, because on ClickHouse Cloud it is the
admin account real people and applications use, and dropping it would hide
genuine cost. If it is only a service account on your cluster, exclude it
explicitly.

Do not try to filter service traffic with the `is_internal` column instead. It
marks anything the server initiated on your behalf, which includes every
refreshable materialized view refresh, so filtering on it can remove the largest
consumer on the cluster.

To exclude additional users (e.g. the `default` user when it is only tooling, or
specific service accounts that pollute the report):

    chq --exclude-users default,my-etl-user --host ...

Or in a config file:

    exclude_users:
      - default
      - my-etl-user

Via env var (comma-separated):

    CHQ_EXCLUDE_USERS=default,my-etl-user chq --host ...

INSERT queries are excluded from `top_n`, `anomalies`, and `cost_attribution`
checks — their ingestion health is covered by the dedicated
`anti_patterns/small_insert_batches` and `cluster_health` checks instead.

Materialized View inner backing tables (`.inner.*`, `.tmp.inner.*`) are
automatically excluded from `cluster_health` checks, since their parts and
merges are driven by MV mechanics, not by user-actionable queries. The cost of
the views themselves is covered by the `materialized_views` category, which reads
the query log rather than part metadata.

## Retention: read this before trusting any number

On ClickHouse Cloud, `system.query_log` is **replica-local** and retained for
**far less time than you would expect**. On four production Cloud services it
held between 0 and 5 days, against 77 to 90 days in an archive table, and
returned only the single replica the client connected to, out of up to 1354 that
had served queries. Measured shares of the same history:

| Service | `system.query_log` | Archive table | Visible via `system.query_log` |
|---|---|---|---|
| A | 5d, 1 of 1354 replicas | 89d | 2.6% |
| B | 0d, 1 of 54 replicas | 77d | 0.26% |
| C | 5d, 1 of 56 replicas | 89d | 5.4% |
| D | 0d, 1 of 38 replicas | 77d | 0.53% |

This does not fail, it returns small plausible numbers. Week-over-week checks are
hit hardest: `anomalies/new_expensive_patterns` compares against a baseline
reaching back `lookback_days * 4` (28 days by default), so on a 5-day log the
baseline is empty and **every** pattern looks new. chq warns when the window it
needs predates the oldest row in the source table.

So on Cloud, set up an archive and point `--table` at it.

Use `chq init` to set up a persistent archive automatically:

    chq init --host ch.example.com --password secret

    # use a custom database
    chq init --host ch.example.com --password secret --database monitoring

This creates:
- A `chq_query_log` MergeTree table (partitioned by month, 90-day TTL)
- A `chq_query_log_mv` materialized view that copies new entries from `system.query_log`
- Backfills existing `system.query_log` data into the new table

Then run chq against the archive:

    chq --table default.chq_query_log --host ch.example.com --password secret

## Custom source table

By default chq reads from `system.query_log`. You can point it at any table
with the same schema using `--table`:

    chq --table my_database.query_log_archive --host ...

Every query-log check honours this. The `cluster_health` and `schema_audit`
checks do not, because they read live state from `system.parts`, `system.merges`,
`system.part_log` and `system.tables` rather than a query log. On a multi-replica
Cloud service those are per-replica and therefore partial: one service reported
2,097 active parts locally against 3,734 distinct parts across its replicas.
Wrapping them in `clusterAllReplicas()` is not a drop-in fix, because replicas
report the same parts and the same merge events, so the naive form double counts
(5,107 rows for those 3,734 distinct parts, and 1.26M part-log rows for 772k
distinct events). Correcting it needs a per-table dedup key, which is not done
yet. Until then, read `cluster_health` absolute numbers as indicative of one
replica rather than as cluster totals.

## Ownership

chq can tell you which table or view is expensive but not who is responsible for
it, since that mapping lives wherever the schema is defined. Supply it with
`--owners-map` (or `CHQ_OWNERS_MAP`) and every row naming a table or a view gains
`owner` and `owner_contact` columns:

    chq --owners-map owners.yml --host ...

```yaml
owners:
  "*.analytics_orders_daily_*": {team: "Orders", contact: "#orders-eng"}
  "*mv_state_history_users*": {team: "Accounts", contact: "#accounts-eng"}
default: {team: "UNOWNED"}
```

Patterns are shell globs matched against the row's table or view name, with
backticks stripped. The longest matching pattern wins, so a specific rule beats a
general one regardless of order in the file. Anything unmatched is reported as
`UNOWNED` rather than dropped, because a missing owner and a table nobody touches
should not look the same.

## Permissions

Read-only. The ClickHouse user needs:

    GRANT SELECT ON system.query_log TO your_user;
    GRANT SELECT ON system.part_log TO your_user;
    GRANT SELECT ON system.parts TO your_user;
    GRANT SELECT ON system.merges TO your_user;
    GRANT SELECT ON system.tables TO your_user;
    GRANT SELECT ON system.asynchronous_metrics TO your_user;
    GRANT SELECT ON system.settings TO your_user;

The `cluster_health` checks query `system.part_log`, `system.parts`,
`system.merges`, `system.tables`, `system.asynchronous_metrics`, and
`system.settings`. If these tables are not accessible, those checks will
be skipped with a warning. All other checks only need `system.query_log`.

If using a custom table:

    GRANT SELECT ON query_log_archive TO your_user;

## Development

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -e '.[dev]'
    python -m pytest tests/ -v

## How it works

Each check is a standalone `.sql` file under `src/chq/sql/`.
At runtime, `{parameter}` placeholders are substituted with your thresholds,
then executed via `clickhouse-connect`. Results are piped to the selected
output formatter (terminal, slack, json, csv, html).

All queries filter on `event_date` (partition key) and `is_initial_query = 1`
to keep scans efficient and avoid double-counting distributed sub-queries.
Queries are grouped by `normalized_query_hash` so structurally identical
statements are aggregated regardless of literal values.

## License

MIT
