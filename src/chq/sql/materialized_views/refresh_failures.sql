-- Check: materialized_views/refresh_failures
-- Detects: Refreshable materialized views whose refreshes are failing, and the CPU
--   and rows they read before giving up.
-- Why it matters: A failing refresh still does all its reading before it dies, so
--   it costs full price and produces nothing. Because ClickHouse retries a
--   refresh, one broken view can burn its whole cost several times per cycle while
--   the target table quietly goes stale. This is usually the cheapest large win on
--   a cluster: the fix is to the view, and the saving is the entire wasted amount.
-- Note: exception_code 241 is a memory limit, which normally means the refresh is
--   trying to materialise its whole source rather than working incrementally.

SELECT
    replaceRegexpOne(extract(log_comment, '^refresh of (.*)$'), ' \\(attempt [0-9]+/[0-9]+\\)$', '') AS mv,
    count()                                             AS failures,
    greatest(max(toUInt8OrZero(extract(log_comment, 'attempt ([0-9]+)/'))), 1) AS attempts,
    any(exception_code)                                 AS exception_code,
    substring(replaceRegexpAll(any(exception), '\\s+', ' '), 1, 160) AS exception,
    toInt64(sum(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6) AS wasted_cpu_seconds,
    formatReadableQuantity(sum(read_rows))              AS rows_read,
    formatReadableSize(sum(read_bytes))                 AS bytes_read,
    formatReadableSize(max(memory_usage))               AS peak_memory
FROM {query_log_table}
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type IN ('ExceptionWhileProcessing', 'ExceptionBeforeStart')
  AND log_comment LIKE 'refresh of %'
  {excluded_users_clause}
GROUP BY mv
ORDER BY wasted_cpu_seconds DESC
LIMIT {top_n}
