-- Check: anti_patterns/full_scans
-- Detects: Queries with a very high read-rows-to-result-rows ratio, indicating
--   they scan far more data than they return.
-- Why it matters: A large scan ratio usually means missing indexes, wrong
--   ORDER BY key, or absent WHERE clauses. Fixing these can cut I/O by orders
--   of magnitude.

SELECT
    if(topK(1)(user)[1] = '', '<system>', topK(1)(user)[1]) AS primary_user,
    count()                                             AS executions,
    toInt64(avg(read_rows))                               AS avg_read_rows,
    toInt64(avg(result_rows))                             AS avg_result_rows,
    round(avg(read_rows) / avg(result_rows), 2)        AS scan_ratio,
    formatReadableSize(avg(read_bytes))                 AS avg_read,
    formatQuery(any(query))                             AS query_text
FROM {query_log_table}
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND result_rows > 0
  AND query NOT LIKE '%system.query_log%'
GROUP BY normalized_query_hash
HAVING scan_ratio > {scan_ratio_threshold}
ORDER BY count() * avg(read_rows) DESC
LIMIT {top_n}
