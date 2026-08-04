-- Check: materialized_views/refresh_wow
-- Detects: Refreshable materialized views whose CPU cost grew sharply against the
--   previous window, plus views that did not run at all before and are already
--   expensive.
-- Why it matters: This is the deploy-regression detector. A view written against a
--   small table behaves well until the source grows or the definition changes, and
--   the absolute cost ranking will not show that something changed this week. A
--   view appearing as NEW at the top of this list is almost always a recent
--   deployment whose cost nobody has looked at yet.
-- Note: views below min_cpu_seconds in the current window are dropped, otherwise
--   the ranking fills up with trivial views moving from a fraction of a second to a
--   couple of seconds.

WITH this_window AS (
    SELECT
        replaceRegexpOne(extract(log_comment, '^refresh of (.*)$'), ' \\(attempt [0-9]+/[0-9]+\\)$', '') AS mv,
        sum(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) AS cpu_us,
        sum(read_rows)                                  AS rows_read,
        count()                                         AS refreshes
    FROM {query_log_table}
    WHERE event_date >= today() - {lookback_days}
      AND is_initial_query = 1
      AND type = 'QueryFinish'
      AND log_comment LIKE 'refresh of %'
      {excluded_users_clause}
    GROUP BY mv
    HAVING cpu_us >= {min_cpu_seconds} * 1e6
),
prev_window AS (
    SELECT
        replaceRegexpOne(extract(log_comment, '^refresh of (.*)$'), ' \\(attempt [0-9]+/[0-9]+\\)$', '') AS mv,
        sum(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) AS cpu_us,
        sum(read_rows)                                  AS rows_read
    FROM {query_log_table}
    WHERE event_date >= today() - {lookback_days} * 2
      AND event_date < today() - {lookback_days}
      AND is_initial_query = 1
      AND type = 'QueryFinish'
      AND log_comment LIKE 'refresh of %'
      {excluded_users_clause}
    GROUP BY mv
)
SELECT
    tw.mv                                               AS mv,
    toInt64(pw.cpu_us / 1e6)                            AS prev_cpu_seconds,
    toInt64(tw.cpu_us / 1e6)                            AS curr_cpu_seconds,
    if(pw.cpu_us = 0, 'NEW', concat(toString(round(tw.cpu_us / pw.cpu_us, 1)), 'x')) AS cpu_change,
    formatReadableQuantity(pw.rows_read)                AS prev_rows_read,
    formatReadableQuantity(tw.rows_read)                AS curr_rows_read,
    tw.refreshes                                        AS refreshes
FROM this_window AS tw
LEFT JOIN prev_window AS pw ON tw.mv = pw.mv
WHERE tw.cpu_us > greatest(pw.cpu_us, 1) * (1 + {regression_threshold_pct} / 100.0)
ORDER BY tw.cpu_us / greatest(pw.cpu_us, 1) DESC
LIMIT {top_n}
