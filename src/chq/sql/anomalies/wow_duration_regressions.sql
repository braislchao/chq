-- Check: anomalies/wow_duration_regressions
-- Detects: Query patterns whose p95 latency regressed week-over-week beyond a
--   configurable threshold.
-- Why it matters: Catching regressions early — before they compound with traffic
--   growth — prevents incidents and keeps SLOs intact.
-- Note: INSERT queries are excluded — their latency is better tracked via
--   cluster_health/insert_duration_by_engine.

WITH this_week AS (
    SELECT
        normalized_query_hash,
        formatQuery(any(query))            AS query_text,
        if(topK(1)(user)[1] = '', '<system>', topK(1)(user)[1]) AS primary_user,
        toInt64(quantile(0.95)(query_duration_ms))          AS p95_ms,
        toInt64(avg(read_bytes))                          AS avg_bytes
    FROM {query_log_table}
    WHERE event_date >= today() - {lookback_days}
      AND is_initial_query = 1
      AND type = 'QueryFinish'
      AND query_kind != 'Insert'
      AND query NOT LIKE '%system.query_log%'
      {excluded_users_clause}
    GROUP BY normalized_query_hash
    HAVING count() >= {min_executions}
),
last_week AS (
    SELECT
        normalized_query_hash,
        toInt64(quantile(0.95)(query_duration_ms))          AS p95_ms,
        toInt64(avg(read_bytes))                          AS avg_bytes
    FROM {query_log_table}
    WHERE event_date >= today() - {lookback_days} * 2
      AND event_date < today() - {lookback_days}
      AND is_initial_query = 1
      AND type = 'QueryFinish'
      AND query_kind != 'Insert'
      AND query NOT LIKE '%system.query_log%'
      {excluded_users_clause}
    GROUP BY normalized_query_hash
    HAVING count() >= {min_executions}
)
SELECT
    tw.primary_user,
    lw.p95_ms                                           AS prev_p95_ms,
    tw.p95_ms                                           AS curr_p95_ms,
    round((tw.p95_ms - lw.p95_ms) / lw.p95_ms * 100, 2) AS duration_change_pct,
    formatReadableSize(lw.avg_bytes)                    AS prev_avg_read,
    formatReadableSize(tw.avg_bytes)                    AS curr_avg_read,
    round((tw.avg_bytes - lw.avg_bytes) / lw.avg_bytes * 100, 2) AS bytes_change_pct,
    tw.query_text
FROM this_week AS tw
INNER JOIN last_week AS lw ON tw.normalized_query_hash = lw.normalized_query_hash
WHERE tw.p95_ms > lw.p95_ms * (1 + {regression_threshold_pct} / 100.0)
ORDER BY duration_change_pct DESC
LIMIT {top_n}
