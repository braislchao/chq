-- Check: materialized_views/refresh_waste_summary
-- Detects: How much of this cluster's refresh work produced nothing, as one row.
-- Why it matters: The per-view checks say which view to fix. This one says whether
--   the cluster has a problem worth anyone's attention, which is the number a
--   report leads with and the number to watch week over week. A cluster spending
--   more CPU on failed refreshes than successful ones is not a tuning question,
--   it is broken.
-- Note: one row, so it is a summary rather than a ranking. Read waste_pct first:
--   under about 5% is background noise from occasional retries, while anything
--   above a quarter means refreshes are routinely dying rather than occasionally.
-- Note: counts every refresh, not just the top_n views, so these totals are the
--   cluster's real figures and will exceed the sum of any ranked check above.

SELECT
    countIf(type = 'QueryFinish')                       AS refreshes,
    uniqExactIf(mv, type = 'QueryFinish')               AS views,
    toInt64(sumIf(cpu_us, type = 'QueryFinish') / 1e6)  AS useful_cpu_seconds,
    toInt64(sumIf(cpu_us, type != 'QueryFinish') / 1e6) AS wasted_cpu_seconds,
    round(sumIf(cpu_us, type != 'QueryFinish') / greatest(sum(cpu_us), 1) * 100, 1) AS waste_pct,
    countIf(type != 'QueryFinish')                      AS failures,
    uniqExactIf(mv, type != 'QueryFinish')              AS failing_views,
    formatReadableQuantity(sumIf(read_rows, type != 'QueryFinish')) AS rows_read_by_failures,
    formatReadableSize(sumIf(read_bytes, type != 'QueryFinish'))    AS bytes_read_by_failures
FROM (
    SELECT
        type,
        read_rows,
        read_bytes,
        replaceRegexpOne(extract(log_comment, '^refresh of (.*)$'), ' \\(attempt [0-9]+/[0-9]+\\)$', '') AS mv,
        ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'] AS cpu_us
    FROM {query_log_table}
    WHERE event_date >= today() - {lookback_days}
      AND is_initial_query = 1
      AND type IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart')
      AND log_comment LIKE 'refresh of %'
      {excluded_users_clause}
)
