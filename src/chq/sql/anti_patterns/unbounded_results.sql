-- Check: anti_patterns/unbounded_results
-- Detects: SELECT queries returning a large number of rows without a LIMIT clause.
-- Why it matters: Unbounded result sets consume excessive memory on both server
--   and client, saturate the network, and often signal a missing pagination or
--   aggregation step in the application.

SELECT
    if(topK(1)(user)[1] = '', '<system>', topK(1)(user)[1]) AS primary_user,
    count()                                             AS executions,
    toInt64(avg(result_rows))                             AS avg_result_rows,
    max(result_rows)                                    AS max_result_rows,
    formatReadableSize(avg(read_bytes))                 AS avg_read,
    formatQuery(any(query))                             AS query_text
FROM {query_log_table}
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
