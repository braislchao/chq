-- Check: cluster_health/insert_duration_by_engine
-- Detects: Insert latency distribution across storage engines.
-- Why it matters: Understanding how insert duration varies by engine and
--   latency bucket helps identify engines or workloads where inserts are
--   unexpectedly slow. High average insert cost (time per row) signals
--   inefficient batching, wrong engine choice, or resource contention
--   during ingestion.

SELECT
    ifNull(engine, 'UNKNOWN')                                   AS engine,
    duration_bucket,
    inserts,
    round(100.0 * inserts / nullIf(sum(inserts) OVER (), 0), 2) AS pct,
    round(avg_rows_per_insert, 1)                               AS avg_rows,
    round(avg_duration_s, 3)                                    AS avg_query_s,
    bar(inserts, 0, max(inserts) OVER (), 60)                   AS bar_chart
FROM
(
    SELECT
        t.engine,
        multiIf(
            q.query_duration_ms < 100,     '<0.1s',
            q.query_duration_ms < 200,     '<0.2s',
            q.query_duration_ms < 500,     '<0.5s',
            q.query_duration_ms < 1000,    '<1s',
            q.query_duration_ms < 2000,    '<2s',
            q.query_duration_ms < 5000,    '<5s',
            q.query_duration_ms < 10000,   '<10s',
            q.query_duration_ms < 30000,   '<30s',
            q.query_duration_ms < 60000,   '<60s',
                                           '>=60s'
        ) AS duration_bucket,
        count()                           AS inserts,
        avg(q.query_duration_ms) / 1000.0 AS avg_duration_s,
        sum(q.written_rows)               AS total_rows,
        sum(q.written_rows) / count()     AS avg_rows_per_insert
    FROM
    (
        SELECT
            event_time,
            query_duration_ms,
            written_rows,
            written_bytes,
            query,
            arrayElement(tables, 1) AS tbl,
            current_database        AS target_db,
            replaceAll(substring(tbl, position(tbl, '.') + 1), '`', '') AS target_table
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND query_kind = 'Insert'
          AND event_time >= now() - INTERVAL {lookback_days} DAY
          AND length(tables) > 0
          AND written_rows > 0
          -- exclude MV inner backing tables (driven by <system>, not user queries)
          AND NOT match(arrayElement(tables, 1), '\\.(inner|tmp\\.inner)\\.')
    ) AS q
    LEFT JOIN system.tables AS t
      ON t.database = q.target_db
     AND t.name     = q.target_table
    GROUP BY t.engine, duration_bucket
) AS buckets
ORDER BY
    engine,
    avg_duration_s
