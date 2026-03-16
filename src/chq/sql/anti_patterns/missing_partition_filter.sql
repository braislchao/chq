-- Check: anti_patterns/missing_partition_filter
-- Detects: Queries scanning an excessive number of parts, suggesting the WHERE
--   clause does not align with the table's partition key.
-- Why it matters: Without a partition filter ClickHouse must open and read from
--   many more parts than necessary, increasing latency and I/O. Adding a
--   partition predicate can eliminate the majority of disk access.

SELECT
    normalized_query_hash,
    substring(any(query), 1, 200)                      AS example_query,
    count()                                             AS executions,
    avg(ProfileEvents['SelectedParts'])                 AS avg_selected_parts,
    avg(read_rows)                                      AS avg_read_rows,
    formatReadableSize(avg(read_bytes))                 AS avg_read_readable,
    topK(1)(user)[1]                                    AS primary_user
FROM {query_log_table}
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND ProfileEvents['SelectedParts'] > {min_parts_threshold}
  AND query NOT LIKE '%system.query_log%'
GROUP BY normalized_query_hash
ORDER BY avg_selected_parts * executions DESC
LIMIT {top_n}
