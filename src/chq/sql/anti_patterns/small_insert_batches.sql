-- Check: anti_patterns/small_insert_batches
-- Detects: INSERT queries writing very few rows per execution.
-- Why it matters: ClickHouse is optimised for bulk ingestion. Many tiny inserts
--   create excessive parts, trigger constant merges, and can lead to the
--   "too many parts" error. Batching inserts to at least tens of thousands of
--   rows dramatically improves throughput and cluster health.

SELECT
    normalized_query_hash,
    substring(any(query), 1, 200)                      AS example_query,
    count()                                             AS executions,
    avg(written_rows)                                   AS avg_written_rows,
    sum(written_rows)                                   AS total_written_rows,
    topK(1)(user)[1]                                    AS primary_user
FROM system.query_log
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type = 'QueryFinish'
  AND query_kind = 'Insert'
  AND written_rows < {min_batch_size}
  AND query NOT LIKE '%system.query_log%'
GROUP BY normalized_query_hash
ORDER BY executions DESC
LIMIT {top_n}
