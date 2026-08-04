-- Check: cost_attribution/select_pct_by_db_and_engine
-- Detects: Distribution of SELECT queries by database and storage engine.
-- Why it matters: Understanding which databases and engine types receive the
--   most SELECT traffic helps pinpoint where read pressure is concentrated.
--   A high percentage of selects hitting full-scan engines or unexpected
--   databases can indicate missing indexes, wrong engine choice, or queries
--   running against the wrong table. The per-database breakdown shows which
--   engine types dominate each database's read workload.

SELECT
    q.target_db                                                                           AS database,
    ifNull(t.engine, 'UNKNOWN')                                                           AS engine,
    count()                                                                               AS select_queries,
    round(100.0 * count() / nullIf(sum(count()) OVER (PARTITION BY q.target_db), 0), 2)  AS pct_of_db,
    round(100.0 * count() / nullIf(sum(count()) OVER (), 0), 2)                           AS pct_of_total,
    formatReadableSize(sum(q.read_bytes))                                                 AS total_read,
    round(avg(q.query_duration_ms) / 1000.0, 3)                                           AS avg_duration_s,
    bar(count(), 0, max(count()) OVER (PARTITION BY q.target_db), 40)                     AS bar_chart
FROM
(
    SELECT
        query_duration_ms,
        read_bytes,
        arrayElement(tables, 1)                                                  AS tbl,
        current_database                                                          AS target_db,
        replaceAll(substring(tbl, position(tbl, '.') + 1), '`', '')              AS target_table
    FROM {query_log_table}
    WHERE type = 'QueryFinish'
      AND query_kind = 'Select'
      AND event_time >= now() - INTERVAL {lookback_days} DAY
      AND is_initial_query = 1
      AND length(tables) > 0
      {excluded_users_clause}
) AS q
LEFT JOIN system.tables AS t
  ON t.database = q.target_db
 AND t.name     = q.target_table
GROUP BY q.target_db, t.engine
ORDER BY q.target_db, select_queries DESC
LIMIT {top_n}
