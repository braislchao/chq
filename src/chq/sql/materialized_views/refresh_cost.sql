-- Check: materialized_views/refresh_cost
-- Detects: Refreshable materialized views ranked by CPU consumed, with the read
--   amplification each one pays to produce its output.
-- Why it matters: A refreshable MV that re-scans its sources on every refresh is
--   invisible in per-user cost reports, because refreshes run with an empty user,
--   yet it is frequently the single largest consumer on the cluster. The
--   amplification column is the signal: a view doing genuinely incremental work
--   sits near 1x, while a view doing a full scan to emit a handful of rows reaches
--   thousands. High amplification plus high CPU is the case worth escalating to
--   whoever owns the view.
-- Note: retry attempts are folded into their parent view, since ClickHouse appends
--   " (attempt N/M)" to log_comment. CPU burnt by refreshes that never succeeded is
--   broken out separately by materialized_views/refresh_failures.

SELECT
    replaceRegexpOne(extract(log_comment, '^refresh of (.*)$'), ' \\(attempt [0-9]+/[0-9]+\\)$', '') AS mv,
    count()                                             AS refreshes,
    toInt64(sum(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6) AS cpu_seconds,
    formatReadableQuantity(sum(read_rows))              AS rows_read,
    formatReadableSize(sum(read_bytes))                 AS bytes_read,
    formatReadableQuantity(sum(written_rows))           AS rows_written,
    toInt64(sum(read_rows) / greatest(sum(written_rows), 1)) AS amplification,
    toInt64(avg(query_duration_ms))                     AS avg_ms,
    formatReadableSize(max(memory_usage))               AS peak_memory
FROM {query_log_table}
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type = 'QueryFinish'
  AND log_comment LIKE 'refresh of %'
  {excluded_users_clause}
GROUP BY mv
ORDER BY cpu_seconds DESC
LIMIT {top_n}
