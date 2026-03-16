-- Check: top_n/by_duration
-- Detects: Queries with the highest p95 latency.
-- Why it matters: High-latency queries hurt user experience and hold server
--   threads longer, reducing concurrency headroom for the rest of the workload.

SELECT
    normalized_query_hash,
    substring(any(query), 1, 200)                      AS example_query,
    count()                                             AS executions,
    quantile(0.50)(query_duration_ms)                   AS p50_duration_ms,
    quantile(0.95)(query_duration_ms)                   AS p95_duration_ms,
    max(query_duration_ms)                              AS max_duration_ms,
    topK(1)(user)[1]                                    AS primary_user
FROM {query_log_table}
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND query NOT LIKE '%system.query_log%'
GROUP BY normalized_query_hash
ORDER BY p95_duration_ms DESC
LIMIT {top_n}
