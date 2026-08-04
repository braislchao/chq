-- Check: schema_audit/missing_company_id_sorting_key
-- Detects: Operational MergeTree tables (non-empty, non-test, non-connector)
--   where company_id is absent from the sorting key or no sorting key is defined.
-- Why it matters: Without company_id as a leading key component, every
--   per-company query performs a full-table scan, driving up read I/O and
--   ClickHouse Cloud credit consumption. These are prime candidates for
--   sorting key redesign before migrating off REST query endpoints.

SELECT
    database,
    name                                                          AS table_name,
    engine,
    if(sorting_key IN ('', 'tuple()'), '<none>', sorting_key)    AS sorting_key,
    partition_key,
    formatReadableQuantity(total_rows)                            AS row_count,
    formatReadableSize(total_bytes)                               AS disk_size,
    if(
        sorting_key IN ('', 'tuple()'),
        'no sorting key',
        'company_id missing from key'
    )                                                             AS issue
FROM system.tables
WHERE engine IN (
    'SharedMergeTree',
    'SharedReplacingMergeTree',
    'SharedCoalescingMergeTree',
    'SharedAggregatingMergeTree',
    'MergeTree',
    'ReplacingMergeTree',
    'AggregatingMergeTree',
    'CollapsingMergeTree',
    'SummingMergeTree'
)
  AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA', 'default', 'monitoring')
  AND total_rows > 0
  AND name NOT ILIKE '%test%'
  -- Exclude Refreshable MV watermark state tables (single-row bookkeeping, not query targets)
  AND name NOT ILIKE '%refresh_watermark%'
  -- Exclude Kafka Connect internal state tables
  AND name NOT ILIKE '%_connect_offset%'
  AND name NOT ILIKE '%_connect_config%'
  AND name NOT ILIKE '%_connect_status%'
  AND (
      sorting_key IN ('', 'tuple()')
      OR NOT (sorting_key LIKE '%company_id%')
  )
ORDER BY total_bytes DESC
