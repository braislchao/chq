-- p50 / p95 / p99 query duration per Query API Endpoint.
-- Ordered by p95 descending so the slowest endpoints surface first.
-- Only considers successful executions (type = 'QueryFinish') within the
-- lookback window; endpoints with zero calls are omitted.
SELECT
    splitByChar(':', user)[2]           AS endpoint_id,
    quantile(0.50)(query_duration_ms)   AS p50_ms,
    quantile(0.95)(query_duration_ms)   AS p95_ms,
    quantile(0.99)(query_duration_ms)   AS p99_ms,
    count()                             AS executions,
    any(query)                          AS query_text
FROM {query_log_table}
WHERE event_date >= today() - {lookback_days}
  AND user LIKE 'queryEndpoint:%'
  AND type = 'QueryFinish'
GROUP BY endpoint_id
ORDER BY p95_ms DESC
