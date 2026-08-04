-- Check: cost_attribution/by_user
-- Detects: Per-user breakdown of resource consumption and error rates.
-- Why it matters: Attributing cost to users (service accounts, teams) is the
--   first step toward chargeback, capacity planning, and identifying who to
--   engage when the cluster is under pressure.
-- Note: server-initiated work has an empty user. Refreshable materialized views
--   are split into their own <mv refresh> bucket rather than being lumped into
--   <system>, because on many clusters they are the largest consumer and an
--   unlabelled bucket hides that. Use materialized_views/refresh_cost to break
--   that bucket down per view.

SELECT
    multiIf(user != '', user,
            log_comment LIKE 'refresh of %', '<mv refresh>',
            '<system>')                                 AS user,
    count()                                             AS total_queries,
    formatReadableSize(sum(read_bytes))                 AS total_read,
    round(sum(query_duration_ms) / 1000 / 3600, 2)     AS total_hours,
    countIf(type = 'ExceptionWhileProcessing')          AS errors,
    round(countIf(type = 'ExceptionWhileProcessing') / count() * 100, 2) AS error_rate_pct
FROM {query_log_table}
WHERE event_date >= today() - {lookback_days}
  AND is_initial_query = 1
  AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
  AND query NOT LIKE '%system.query_log%'
  {excluded_users_clause}
GROUP BY user
ORDER BY sum(read_bytes) DESC
