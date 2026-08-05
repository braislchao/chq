-- Check: cost_attribution/by_table
-- Detects: Which tables the cluster actually spends its CPU and reads on.
-- Why it matters: Cost is reported per user and per query pattern, but neither
--   tells you what to fix. Shared service accounts hide behind one name and
--   query hashes mean nothing to the team that owns the data. A table name is
--   the one identifier that maps to an owner, so this is the check to hand to
--   whoever is responsible for the schema. Pair it with --owners-map to get the
--   team name onto every row.
-- Note: this attributes, it does not partition. A query touching five tables
--   contributes its whole cost to each of them, so the column totals add up to
--   more than the cluster did. That is the correct reading for "how expensive is
--   it to serve this table", but do not sum the column and expect the cluster
--   total.
-- Note: uses the tables array rather than current_database, which is only the
--   session default and mis-attributes any query that names its database.

SELECT
    table,
    uniqExact(normalized_query_hash)                    AS query_patterns,
    count()                                             AS executions,
    toInt64(sum(cpu_us) / 1e6)                          AS cpu_seconds,
    formatReadableSize(sum(read_bytes))                 AS total_read,
    formatReadableQuantity(sum(read_rows))              AS rows_read,
    toInt64(quantile(0.95)(query_duration_ms))          AS p95_ms
FROM (
    SELECT
        arrayJoin(tables)                               AS table,
        normalized_query_hash,
        read_bytes,
        read_rows,
        query_duration_ms,
        ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'] AS cpu_us
    FROM {query_log_table}
    WHERE event_date >= today() - {lookback_days}
      AND is_initial_query = 1
      AND type = 'QueryFinish'
      AND query NOT LIKE '%{query_log_table}%'
      {excluded_users_clause}
)
-- Table functions are not tables, MV backing tables are an implementation
-- detail of their view, and the system databases are not anybody's workload.
WHERE table NOT LIKE '\_table\_function.%'
  AND table NOT LIKE '%.inner%'
  AND table NOT LIKE 'system.%'
  AND table NOT LIKE 'INFORMATION_SCHEMA.%'
  AND table NOT LIKE 'information_schema.%'
GROUP BY table
ORDER BY sum(cpu_us) DESC
LIMIT {top_n}
