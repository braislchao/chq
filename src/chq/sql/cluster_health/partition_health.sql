-- Check: cluster_health/partition_health
-- Detects: Partition-level fragmentation, merge backlogs, and oversized parts.
-- Why it matters: Partition health directly affects query performance and merge
--   efficiency. Too many small parts signal a merge backlog or micro-insert
--   pattern. Oversized parts make merges expensive and memory-heavy. Historical
--   partitions that are still merging waste background resources. This check
--   adapts thresholds to node RAM so recommendations are meaningful across
--   different hardware configurations.

WITH
mem_info AS (
    SELECT
        toUInt64(maxIf(value, metric = 'MemoryTotal')) AS total_memory_bytes_async
    FROM system.asynchronous_metrics
),

mem_fallback AS (
    SELECT
        toUInt64OrZero(maxIf(value, name = 'max_memory_usage')) AS max_memory_usage_setting
    FROM system.settings
),

mem_total AS (
    SELECT
        CASE
            WHEN mi.total_memory_bytes_async > 0 THEN mi.total_memory_bytes_async
            WHEN mf.max_memory_usage_setting > 0 THEN mf.max_memory_usage_setting
            ELSE toUInt64(64000000000)
        END AS total_memory_bytes
    FROM mem_info mi
    CROSS JOIN mem_fallback mf
),

thresholds AS (
    SELECT
        total_memory_bytes,
        round(toFloat64(total_memory_bytes) / 1024 / 1024 / 1024, 2) AS total_ram_gb,
        CASE
            WHEN total_memory_bytes < 96e9 THEN toUInt64(1024)
            WHEN total_memory_bytes < 192e9 THEN toUInt64(2048)
            WHEN total_memory_bytes < 512e9 THEN toUInt64(4096)
            ELSE toUInt64(8192)
        END AS oversized_threshold_mb
    FROM mem_total
),

part_stats AS (
    SELECT
        database,
        table,
        partition,
        count()                                                     AS active_parts,
        round(sum(rows) / 1e6, 2)                                  AS total_rows_m,
        round(sum(bytes_on_disk) / 1024 / 1024 / 1024, 2)          AS size_gb,
        round(avg(bytes_on_disk) / 1024 / 1024, 2)                 AS avg_part_size_mb,
        round(count() / nullIf(sum(bytes_on_disk) / 1024 / 1024 / 1024, 0), 0) AS parts_per_gb
    FROM system.parts
    WHERE active AND database NOT IN ('system')
    GROUP BY database, table, partition
    HAVING sum(rows) > 100000 AND sum(bytes_on_disk) > 100000000
),

merge_activity AS (
    SELECT
        database,
        table,
        count() AS active_merges
    FROM system.merges
    GROUP BY database, table
)

SELECT
    ps.database,
    ps.table,
    ps.partition,
    CASE
        WHEN ps.size_gb < 0.1 THEN '⚪ Small / Metadata (ignored)'

        WHEN ((length(partition) = 6 AND toUInt64OrZero(partition) < toUInt64(formatDateTime(addMonths(now(), -2), '%Y%m')))
                OR (length(partition) = 4 AND toUInt64OrZero(partition) < toUInt64(formatDateTime(now(), '%Y'))))
            AND (coalesce(ma.active_merges, 0) > 0 OR ps.parts_per_gb > 100)
            THEN '🟠 Historical (still merging / fragmented)'

        WHEN ((length(partition) = 6 AND toUInt64OrZero(partition) < toUInt64(formatDateTime(addMonths(now(), -2), '%Y%m')))
                OR (length(partition) = 4 AND toUInt64OrZero(partition) < toUInt64(formatDateTime(now(), '%Y'))))
                AND coalesce(ma.active_merges, 0) = 0 AND ps.parts_per_gb <= 100
            THEN '🟤 Archived / Historical (frozen data)'

        WHEN (ps.avg_part_size_mb < 16 OR ps.parts_per_gb > 300) AND coalesce(ma.active_merges, 0) = 0
            THEN '🔴 Merge backlog (too many small parts, no merges)'

        WHEN (ps.avg_part_size_mb < 16 OR ps.parts_per_gb > 300) AND coalesce(ma.active_merges, 0) > 0
            THEN '🟠 Catch-up merging (many small parts, merges active)'

        WHEN (ps.avg_part_size_mb < 64 OR ps.parts_per_gb > 150) AND coalesce(ma.active_merges, 0) > 0
            THEN '🟠 Catch-up merging (moderate fragmentation)'

        WHEN ps.avg_part_size_mb BETWEEN 16 AND 32 AND ps.parts_per_gb <= 50
            THEN '🟢 Healthy / Small table'

        WHEN ps.avg_part_size_mb BETWEEN 32 AND 128 AND ps.parts_per_gb <= 50
            THEN '🟢 Healthy / Efficient batching'

        WHEN ps.avg_part_size_mb BETWEEN 128 AND t.oversized_threshold_mb * 0.8 THEN '🟢 Healthy / Efficient batching'

        WHEN ps.avg_part_size_mb BETWEEN t.oversized_threshold_mb * 0.8 AND t.oversized_threshold_mb * 1.5 THEN '🟢 Large but OK (fits node memory)'

        WHEN ps.avg_part_size_mb BETWEEN t.oversized_threshold_mb * 1.5 AND t.oversized_threshold_mb * 3 THEN '🟡 Oversized (high merge cost)'

        WHEN ps.avg_part_size_mb > t.oversized_threshold_mb * 3 THEN '🟥 Extreme oversize (merge tuning needed)'

        ELSE '⚫ Unclassified (check logic)'
    END AS partition_status,
    ps.total_rows_m,
    ps.size_gb,
    ps.active_parts,
    ps.avg_part_size_mb,
    ps.parts_per_gb,
    coalesce(ma.active_merges, 0) AS active_merges,
    t.total_ram_gb AS node_ram_gb,
    t.oversized_threshold_mb
FROM part_stats ps
LEFT JOIN merge_activity ma USING (database, table)
CROSS JOIN thresholds t
ORDER BY
    CASE
        WHEN partition_status LIKE '🔴%' THEN 1
        WHEN partition_status LIKE '🟠%' THEN 2
        WHEN partition_status LIKE '🟡%' THEN 3
        WHEN partition_status LIKE '🟥%' THEN 4
        WHEN partition_status LIKE '🟢%' THEN 5
        WHEN partition_status LIKE '⚪%' THEN 6
        WHEN partition_status LIKE '🟤%' THEN 7
        ELSE 8
    END, ps.parts_per_gb DESC, ps.size_gb DESC
LIMIT {top_n}
