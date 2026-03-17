-- Check: cluster_health/merge_pressure
-- Detects: Tables with slow, frequent, or high-pressure background merges.
-- Why it matters: Heavy merge churn or long-running merges increase I/O
--   contention, delay other merges, and can saturate the merge pool.
--   Identifying tables under merge pressure helps prioritise tuning of
--   merge_parallelism, insert batching, or partition strategy before
--   the merge backlog causes query degradation or "too many parts" errors.

WITH
    now() AS t_now,
    t_now - INTERVAL {lookback_days} DAY AS t_from

, merges AS (
    SELECT
        database,
        table,
        event_time,
        toFloat64(duration_ms) / 1000.0           AS duration_s,
        length(merged_from)                        AS input_parts,
        rows                                       AS out_rows,
        bytes_uncompressed                         AS out_bytes
    FROM system.part_log
    WHERE event_type = 'MergeParts'
      AND event_time >= t_from
      AND event_time <= t_now
      AND table NOT LIKE '.inner.%'
      AND table NOT LIKE '.tmp.inner.%'
)

, per_table_span AS (
    SELECT
        database,
        table,
        COUNT()           AS merges,
        MIN(event_time)   AS first_merge,
        MAX(event_time)   AS last_merge,
        CAST(
          IF(COUNT() > 1,
             toFloat64(dateDiff('second', MIN(event_time), MAX(event_time))) / (COUNT() - 1),
             NULL),
          'Nullable(Float64)'
        ) AS avg_merge_interval_sec
    FROM merges
    GROUP BY database, table
)

SELECT
    m.database,
    m.table,

    round(quantileExact(0.90)(duration_s), 2)   AS p90_merge_duration_s,
    round(AVGIf(duration_s, duration_s > 0), 2) AS avg_merge_duration_s,
    formatReadableTimeDelta(CAST(quantileExact(0.90)(duration_s) AS Int64)) AS p90_merge_duration_human,

    multiIf(
        quantileExact(0.90)(duration_s) < 5,    '🟢 FAST',
        quantileExact(0.90)(duration_s) < 30,   '🟡 MODERATE',
        quantileExact(0.90)(duration_s) < 120,  '🟠 SLOW',
        '🔴 PAINFUL'
    ) AS merge_duration_class,

    COUNT() AS total_merges,

    multiIf(
        COUNT() < 1e3,   '⚪ LIGHT LOAD',
        COUNT() < 5e3,   '🟡 MEDIUM LOAD',
        COUNT() < 2e4,   '🟠 HEAVY LOAD',
        '🔴 EXTREME LOAD'
    ) AS merge_load_class,

    multiIf(
        quantileExact(0.90)(duration_s) < 10,                                     '🔵 FAST',
        (quantileExact(0.90)(duration_s) < 30  AND COUNT() >= 1e4),               '🟢 MODERATE',
        (quantileExact(0.90)(duration_s) < 120 AND COUNT() >= 1e4),               '🟡 SLOW',
        (quantileExact(0.90)(duration_s) < 1800 AND COUNT() >= 1e4),              '🔴 PAINFUL (2-30 min)',
        (quantileExact(0.90)(duration_s) < 7200 AND COUNT() >= 1e4),              '🟣 SEVERE (30-120 min)',
        (COUNT() >= 1e4),                                                          '🛑 CRITICAL (>= 2 h)',
        '-'
    ) AS merge_pressure_class,

    SUM(input_parts)                          AS total_parts_merged,
    round(AVG(input_parts), 2)                AS avg_parts_per_merge,
    SUM(out_rows)                             AS total_rows_merged,
    ROUND(AVG(out_rows))                      AS avg_rows_per_merge,
    round(SUM(duration_s) / 3600.0, 2)        AS total_merge_wall_time_h,

    s.avg_merge_interval_sec,
    CASE
        WHEN ifNull(s.avg_merge_interval_sec, 1e12) < 10     THEN '<10s'
        WHEN ifNull(s.avg_merge_interval_sec, 1e12) < 60     THEN '<1min'
        WHEN ifNull(s.avg_merge_interval_sec, 1e12) < 300    THEN '<5min'
        WHEN ifNull(s.avg_merge_interval_sec, 1e12) < 900    THEN '<15min'
        WHEN ifNull(s.avg_merge_interval_sec, 1e12) < 3600   THEN 'Hourly'
        WHEN ifNull(s.avg_merge_interval_sec, 1e12) < 86400  THEN 'Daily'
        ELSE 'Rare'
    END AS merge_frequency_class,

    formatReadableTimeDelta(CAST(AVGIf(duration_s, duration_s > 0) AS Int64)) AS avg_merge_duration_human,

    MIN(event_time) AS first_merge_time,
    MAX(event_time) AS last_merge_time

FROM merges m
JOIN per_table_span s
  ON m.database = s.database AND m.table = s.table
GROUP BY
    m.database, m.table, s.avg_merge_interval_sec
ORDER BY
    merge_pressure_class DESC, total_merges DESC, p90_merge_duration_s DESC
LIMIT {top_n}
