-- Check: anomalies/wow_duration_regressions
-- Detects: Query patterns whose p95 latency regressed week-over-week beyond a
--   configurable threshold.
-- Why it matters: Catching regressions early — before they compound with traffic
--   growth — prevents incidents and keeps SLOs intact.

WITH this_week AS (
    SELECT
        normalized_query_hash,
        substring(any(query), 1, 200)                  AS example_query,
        topK(1)(user)[1]                                AS primary_user,
        quantile(0.95)(query_duration_ms)               AS p95_ms,
        avg(read_bytes)                                 AS avg_bytes
    FROM system.query_log
    WHERE event_date >= today() - {lookback_days}
      AND is_initial_query = 1
      AND type = 'QueryFinish'
      AND query NOT LIKE '%system.query_log%'
    GROUP BY normalized_query_hash
    HAVING count() >= {min_executions}
),
last_week AS (
    SELECT
        normalized_query_hash,
        quantile(0.95)(query_duration_ms)               AS p95_ms,
        avg(read_bytes)                                 AS avg_bytes
    FROM system.query_log
    WHERE event_date >= today() - {lookback_days} * 2
      AND event_date < today() - {lookback_days}
      AND is_initial_query = 1
      AND type = 'QueryFinish'
      AND query NOT LIKE '%system.query_log%'
    GROUP BY normalized_query_hash
    HAVING count() >= {min_executions}
)
SELECT
    tw.normalized_query_hash,
    tw.example_query,
    tw.primary_user,
    lw.p95_ms                                           AS prev_p95_ms,
    tw.p95_ms                                           AS curr_p95_ms,
    round((tw.p95_ms - lw.p95_ms) / lw.p95_ms * 100, 2) AS duration_change_pct,
    lw.avg_bytes                                        AS prev_avg_bytes,
    tw.avg_bytes                                        AS curr_avg_bytes,
    round((tw.avg_bytes - lw.avg_bytes) / lw.avg_bytes * 100, 2) AS bytes_change_pct
FROM this_week AS tw
INNER JOIN last_week AS lw ON tw.normalized_query_hash = lw.normalized_query_hash
WHERE tw.p95_ms > lw.p95_ms * (1 + {regression_threshold_pct} / 100.0)
ORDER BY duration_change_pct DESC
LIMIT {top_n}
