-- Check: top_n/by_read_bytes
-- Detects: Queries that scan the most data in aggregate.
-- Why it matters: Total bytes read is the primary driver of I/O pressure and
--   ClickHouse Cloud compute cost. Reducing top scanners yields the biggest
--   resource savings.

SELECT
    if(topK(1)(user)[1] = '', '<system>', topK(1)(user)[1]) AS primary_user,
    count()                                             AS executions,
    formatReadableSize(sum(read_bytes))                 AS total_read,
    formatReadableSize(avg(read_bytes))                 AS avg_read,
    formatQuery(any(query))                             AS query_text
FROM {query_log_table}
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND query NOT LIKE '%system.query_log%'
  {excluded_users_clause}
GROUP BY normalized_query_hash
ORDER BY sum(read_bytes) DESC
LIMIT {top_n}
