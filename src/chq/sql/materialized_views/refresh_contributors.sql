-- Check: materialized_views/refresh_contributors
-- Detects: The tables that the expensive refreshable views have in common, and how
--   many of them scan each one.
-- Why it matters: A view's owner is whoever controls its refresh, but the cost is
--   usually driven by a shared dimension or CTE that several views all scan, owned
--   by somebody else. This names those tables, so "your view is expensive" becomes
--   "these views are expensive and they all read this", which is a fixable
--   statement. A table scanned by many expensive views is worth optimising once
--   instead of working around N times.
--
-- Note on what this deliberately does NOT do: it does not attribute rows or CPU to
--   individual source tables, because the query log cannot support it. read_rows is
--   recorded per query, not per table, so exploding the tables array and summing
--   gives every source of a refresh the refresh's entire total. Any per-table cost
--   figure derived that way is an artefact. Only the fan-out is real, so only the
--   fan-out is reported.
--
-- Note: `views_scanning` is the column to read. `combined_cpu_seconds` is the total
--   CPU of the views that scan this table, NOT this table's own cost, and the same
--   CPU appears against every source of a given view. Use it to rank, never to sum.

SELECT
    source_table,
    uniqExact(mv)                                       AS views_scanning,
    toInt64(sum(cpu_us) / 1e6)                          AS combined_cpu_seconds,
    countIf(failed)                                     AS failed_refreshes,
    arrayStringConcat(arraySlice(arraySort(groupUniqArray(mv)), 1, 4), ', ') AS example_views
FROM (
    SELECT
        replaceRegexpOne(extract(log_comment, '^refresh of (.*)$'), ' \\(attempt [0-9]+/[0-9]+\\)$', '') AS mv,
        arrayJoin(tables)                               AS source_table,
        ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'] AS cpu_us,
        type != 'QueryFinish'                           AS failed
    FROM {query_log_table}
    WHERE event_date >= today() - {lookback_days}
      AND is_initial_query = 1
      AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
      AND log_comment LIKE 'refresh of %'
      {excluded_users_clause}
)
-- Table functions are not tables, MV inner tables are an implementation detail, and
-- a view's own target is not one of its sources.
WHERE source_table NOT LIKE '\_table\_function.%'
  AND source_table NOT LIKE '%.inner%'
  AND NOT position(mv, replaceAll(source_table, '`', '')) > 0
GROUP BY source_table
HAVING views_scanning > 1
ORDER BY views_scanning DESC, combined_cpu_seconds DESC
LIMIT {top_n}
