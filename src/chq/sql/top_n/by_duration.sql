-- Check: top_n/by_duration
-- Detects: Queries with the highest p95 latency.
-- Why it matters: High-latency queries hurt user experience and hold server
--   threads longer, reducing concurrency headroom for the rest of the workload.

SELECT
    if(topK(1)(user)[1] = '', '<system>', topK(1)(user)[1]) AS primary_user,
    count()                                             AS executions,
    toInt64(quantile(0.50)(query_duration_ms))            AS p50_ms,
    toInt64(quantile(0.95)(query_duration_ms))            AS p95_ms,
    max(query_duration_ms)                              AS max_ms,
    formatQuery(any(query))                             AS example_query
FROM {query_log_table}
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND query NOT LIKE '%system.query_log%'
GROUP BY normalized_query_hash
ORDER BY p95_ms DESC
LIMIT {top_n}
