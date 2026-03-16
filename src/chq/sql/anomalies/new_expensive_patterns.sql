-- Check: anomalies/new_expensive_patterns
-- Detects: Query patterns that appeared for the first time this week and are
--   already expensive (avg read > 100 MB or avg duration > 10 s).
-- Why it matters: New workloads often ship without proper tuning. Catching them
--   in their first week gives teams the earliest possible window to optimise
--   before the pattern becomes entrenched.

WITH this_week AS (
    SELECT
        normalized_query_hash,
        formatQuery(any(query))            AS example_query,
        count()                                         AS executions,
        toInt64(avg(query_duration_ms))                     AS avg_duration_ms,
        toInt64(avg(read_bytes))                          AS avg_read_bytes,
        formatReadableSize(avg(read_bytes))             AS avg_read_readable,
        if(topK(1)(user)[1] = '', '<system>', topK(1)(user)[1]) AS primary_user
    FROM {query_log_table}
    WHERE event_date >= today() - {lookback_days}
      AND is_initial_query = 1
      AND type = 'QueryFinish'
      AND query NOT LIKE '%system.query_log%'
    GROUP BY normalized_query_hash
    HAVING avg_read_bytes > 100000000 OR avg_duration_ms > 10000
),
baseline AS (
    SELECT DISTINCT normalized_query_hash
    FROM {query_log_table}
    WHERE event_date >= today() - {lookback_days} * 4
      AND event_date < today() - {lookback_days}
      AND is_initial_query = 1
      AND type = 'QueryFinish'
      AND query NOT LIKE '%system.query_log%'
)
SELECT
    tw.primary_user,
    tw.executions,
    tw.avg_duration_ms,
    tw.avg_read_readable                                AS avg_read,
    tw.example_query
FROM this_week AS tw
LEFT ANTI JOIN baseline AS bl ON tw.normalized_query_hash = bl.normalized_query_hash
ORDER BY tw.avg_read_bytes DESC
LIMIT {top_n}
