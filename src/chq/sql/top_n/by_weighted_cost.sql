-- Check: top_n/by_weighted_cost
-- Detects: Queries with the highest composite cost (executions * avg bytes read).
-- Why it matters: A single cheap query repeated thousands of times can cost more
--   than one heavy outlier. This "death by a thousand cuts" metric surfaces
--   patterns that top-by-peak rankings miss.

SELECT
    normalized_query_hash,
    substring(any(query), 1, 200)                      AS example_query,
    count()                                             AS executions,
    avg(read_bytes)                                     AS avg_read_bytes,
    count() * avg(read_bytes)                           AS weighted_cost,
    formatReadableSize(count() * avg(read_bytes))       AS weighted_cost_readable,
    topK(1)(user)[1]                                    AS primary_user
FROM system.query_log
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type = 'QueryFinish'
GROUP BY normalized_query_hash
ORDER BY weighted_cost DESC
LIMIT {top_n}
