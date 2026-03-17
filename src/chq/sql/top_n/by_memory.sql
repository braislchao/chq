-- Check: top_n/by_memory
-- Detects: Queries with the highest peak memory consumption.
-- Why it matters: Memory-heavy queries can trigger OOM kills, evict useful
--   caches, and degrade performance for neighbouring workloads on shared
--   ClickHouse Cloud nodes.

SELECT
    if(topK(1)(user)[1] = '', '<system>', topK(1)(user)[1]) AS primary_user,
    count()                                             AS executions,
    formatReadableSize(max(memory_usage))               AS peak_memory,
    formatReadableSize(quantile(0.95)(memory_usage))    AS p95_memory,
    formatQuery(any(query))                             AS query_text
FROM {query_log_table}
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND query NOT LIKE '%system.query_log%'
  {excluded_users_clause}
GROUP BY normalized_query_hash
ORDER BY max(memory_usage) DESC
LIMIT {top_n}
