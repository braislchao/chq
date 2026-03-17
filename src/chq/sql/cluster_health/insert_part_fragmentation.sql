-- Check: cluster_health/insert_part_fragmentation
-- Detects: Tables generating too many small parts from direct inserts.
-- Why it matters: Excessive small parts are the main source of performance
--   degradation and merge load in MergeTree engines. Tables with high
--   fragmentation risk hitting the "too many parts" error, suffer elevated
--   I/O from constant background merges, and degrade query latency.
--   Batching inserts or enabling async_insert can dramatically reduce
--   part churn.
-- Note: .inner. and .tmp.inner. tables (hidden backing tables for Materialized
--   Views created by <system>) are excluded — they receive INSERTs driven by
--   their source tables and are not user-actionable.

WITH
    1000 AS SMALL_ROWS,
    100  AS TINY_ROWS

, base AS (
    SELECT
        database,
        table,
        partition_id,
        part_name,
        event_time,
        rows
    FROM system.part_log
    WHERE event_type = 'NewPart'
      AND event_time > now() - INTERVAL {lookback_days} DAY
      AND length(merged_from) = 0
      AND table NOT LIKE '.inner.%'
      AND table NOT LIKE '.tmp.inner.%'
)

, dedup AS (
    SELECT
        database,
        table,
        partition_id,
        part_name,
        MIN(event_time) AS event_time,
        MAX(rows)       AS rows
    FROM base
    GROUP BY database, table, partition_id, part_name
)

, per_table AS (
    SELECT
        database,
        table,
        COUNT()                    AS added_parts,
        SUM(rows)                  AS total_rows,
        ROUND(AVG(rows))           AS avg_rows_per_part,
        quantileExact(0.5)(rows)   AS p50_rows_per_part,
        countIf(rows < TINY_ROWS)  AS tiny_parts,
        countIf(rows < SMALL_ROWS) AS small_parts,
        ROUND(100 * countIf(rows < SMALL_ROWS) / COUNT(), 2) AS small_parts_pct,
        CAST(
            IF(COUNT() > 1,
               toFloat64(dateDiff('second', MIN(event_time), MAX(event_time)))
               / (COUNT() - 1),
               NULL),
            'Nullable(Float64)'
        ) AS avg_interval_sec_f
    FROM dedup
    GROUP BY database, table
)

SELECT
    database,
    table,
    added_parts,
    total_rows,
    CASE
        WHEN avg_rows_per_part < 1e2  THEN '🟥 VERY SMALL'
        WHEN avg_rows_per_part < 1e3  THEN '🟧 SMALL'
        WHEN avg_rows_per_part < 1e4  THEN '🟩 NORMAL'
        WHEN avg_rows_per_part < 1e5  THEN '🟦 LARGE'
        WHEN avg_rows_per_part < 1e6  THEN '🟪 X LARGE'
        ELSE '🟫 XX LARGE'
    END AS batch_size_class,
    round(avg_interval_sec_f, 1) AS avg_interval_sec,
    CASE
        WHEN ifNull(avg_interval_sec_f, 1e12) < 10      THEN '<10s'
        WHEN ifNull(avg_interval_sec_f, 1e12) < 60      THEN '<1min'
        WHEN ifNull(avg_interval_sec_f, 1e12) < 300     THEN '<5min'
        WHEN ifNull(avg_interval_sec_f, 1e12) < 900     THEN '<15min'
        WHEN ifNull(avg_interval_sec_f, 1e12) < 1800    THEN '<30min'
        WHEN ifNull(avg_interval_sec_f, 1e12) < 3600    THEN 'Hourly'
        WHEN ifNull(avg_interval_sec_f, 1e12) < 86400   THEN 'Daily'
        ELSE 'Rare'
    END AS insert_frequency_class,
    avg_rows_per_part,
    p50_rows_per_part,
    small_parts_pct
FROM per_table
ORDER BY added_parts DESC
LIMIT {top_n}
