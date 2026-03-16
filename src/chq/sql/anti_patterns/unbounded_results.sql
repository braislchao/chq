-- Check: anti_patterns/unbounded_results
-- Detects: SELECT queries returning a large number of rows without a LIMIT clause.
-- Why it matters: Unbounded result sets consume excessive memory on both server
--   and client, saturate the network, and often signal a missing pagination or
--   aggregation step in the application.

SELECT
    normalized_query_hash,
    substring(any(query), 1, 200)                      AS example_query,
    count()                                             AS executions,
    avg(result_rows)                                    AS avg_result_rows,
    max(result_rows)                                    AS max_result_rows,
    formatReadableSize(avg(read_bytes))                 AS avg_read_readable,
    topK(1)(user)[1]                                    AS primary_user
FROM system.query_log
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND result_rows > {max_result_rows}
  AND NOT match(query, '(?i)\\bLIMIT\\s+\\d+')
  AND query NOT LIKE '%system.query_log%'
GROUP BY normalized_query_hash
ORDER BY avg_result_rows DESC
LIMIT {top_n}
