-- Check: top_n/by_memory
-- Detects: Queries with the highest peak memory consumption.
-- Why it matters: Memory-heavy queries can trigger OOM kills, evict useful
--   caches, and degrade performance for neighbouring workloads on shared
--   ClickHouse Cloud nodes.

SELECT
    normalized_query_hash,
    substring(any(query), 1, 200)                      AS example_query,
    count()                                             AS executions,
    max(memory_usage)                                   AS peak_memory,
    quantile(0.95)(memory_usage)                        AS p95_memory,
    formatReadableSize(max(memory_usage))               AS peak_memory_readable,
    topK(1)(user)[1]                                    AS primary_user
FROM {query_log_table}
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND query NOT LIKE '%system.query_log%'
GROUP BY normalized_query_hash
ORDER BY peak_memory DESC
LIMIT {top_n}
