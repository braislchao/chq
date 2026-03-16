-- Check: top_n/by_weighted_cost
-- Detects: Queries with the highest composite cost (executions * avg bytes read).
-- Why it matters: A single cheap query repeated thousands of times can cost more
--   than one heavy outlier. This "death by a thousand cuts" metric surfaces
--   patterns that top-by-peak rankings miss.

SELECT
    if(topK(1)(user)[1] = '', '<system>', topK(1)(user)[1]) AS primary_user,
    count()                                             AS executions,
    formatReadableSize(avg(read_bytes))                 AS avg_read,
    formatReadableSize(count() * avg(read_bytes))       AS weighted_cost,
    formatQuery(any(query))                             AS example_query
FROM {query_log_table}
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type = 'QueryFinish'
  AND query NOT LIKE '%system.query_log%'
GROUP BY normalized_query_hash
ORDER BY count() * avg(read_bytes) DESC
LIMIT {top_n}
