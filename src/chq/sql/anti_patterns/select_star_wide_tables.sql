-- Check: anti_patterns/select_star_wide_tables
-- Detects: Queries using SELECT * patterns.
-- Why it matters: SELECT * on wide tables reads every column from disk, wastes
--   network bandwidth, and defeats ClickHouse's columnar compression advantage.
--   Projecting only needed columns often cuts read volume dramatically.

SELECT
    normalized_query_hash,
    substring(any(query), 1, 200)                      AS example_query,
    count()                                             AS executions,
    avg(read_bytes)                                     AS avg_read_bytes,
    formatReadableSize(avg(read_bytes))                 AS avg_read_readable,
    topK(1)(user)[1]                                    AS primary_user
FROM {query_log_table}
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND match(query, '(?i)\\bSELECT\\s+\\*\\s+FROM\\b')
  AND query NOT LIKE '%system.query_log%'
GROUP BY normalized_query_hash
ORDER BY executions DESC
LIMIT {top_n}
