-- Data scanned per call for each Query API Endpoint.
-- avg_read_readable and total_read_readable use ClickHouse's built-in
-- formatReadableSize() for human-friendly output (KiB / MiB / GiB / TiB).
-- max_read_readable highlights outlier calls that scan far more than average.
-- Ordered by avg bytes scanned descending so the costliest endpoints surface first.
SELECT
    splitByChar(':', user)[2]               AS endpoint_id,
    count()                                 AS executions,
    formatReadableSize(avg(read_bytes))     AS avg_read_readable,
    formatReadableSize(sum(read_bytes))     AS total_read_readable,
    formatReadableSize(max(read_bytes))     AS max_read_readable,
    any(query)                              AS query_text
FROM {query_log_table}
WHERE event_date >= today() - {lookback_days}
  AND user LIKE 'queryEndpoint:%'
  AND type = 'QueryFinish'
GROUP BY endpoint_id
ORDER BY avg(read_bytes) DESC
