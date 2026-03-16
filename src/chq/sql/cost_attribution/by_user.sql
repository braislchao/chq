-- Check: cost_attribution/by_user
-- Detects: Per-user breakdown of resource consumption and error rates.
-- Why it matters: Attributing cost to users (service accounts, teams) is the
--   first step toward chargeback, capacity planning, and identifying who to
--   engage when the cluster is under pressure.

SELECT
    user,
    count()                                             AS total_queries,
    sum(read_bytes)                                     AS total_read_bytes,
    formatReadableSize(sum(read_bytes))                 AS total_read_readable,
    sum(query_duration_ms)                              AS total_duration_ms,
    round(sum(query_duration_ms) / 1000 / 3600, 2)     AS total_hours,
    countIf(type = 'ExceptionWhileProcessing')          AS errors,
    round(countIf(type = 'ExceptionWhileProcessing') / count() * 100, 2) AS error_rate_pct
FROM system.query_log
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
GROUP BY user
ORDER BY total_read_bytes DESC
