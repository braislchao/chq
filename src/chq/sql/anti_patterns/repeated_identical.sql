-- Check: anti_patterns/repeated_identical
-- Detects: The same normalised query running at very high frequency (more than
--   {repeat_threshold} times per hour on average).
-- Why it matters: Excessive repetition of identical queries is a strong signal
--   of a missing cache layer, a polling loop with too-short intervals, or an
--   application retry storm. Consolidating or caching these calls can free
--   significant cluster capacity.

SELECT
    normalized_query_hash,
    substring(any(query), 1, 200)                      AS example_query,
    count()                                             AS total_executions,
    round(count() / {lookback_days} / 24, 2)           AS avg_per_hour,
    avg(read_bytes)                                     AS avg_read_bytes,
    formatReadableSize(avg(read_bytes))                 AS avg_read_readable,
    topK(1)(user)[1]                                    AS primary_user
FROM system.query_log
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type = 'QueryFinish'
  AND query NOT LIKE '%system.query_log%'
GROUP BY normalized_query_hash
HAVING avg_per_hour > {repeat_threshold}
ORDER BY total_executions DESC
LIMIT {top_n}
