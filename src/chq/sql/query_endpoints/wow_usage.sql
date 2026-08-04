-- Week-over-week call volume for Query API Endpoints.
-- Endpoints are identified via user LIKE 'queryEndpoint:<uuid>'.
-- "This week" = last {lookback_days} days; "last week" = the prior window of the same length.
WITH
this_week AS (
    SELECT
        splitByChar(':', user)[2]   AS endpoint_id,
        count()                     AS calls_this_week
    FROM {query_log_table}
    WHERE event_date >= today() - {lookback_days}
      AND user LIKE 'queryEndpoint:%'
      AND type = 'QueryFinish'
    GROUP BY endpoint_id
),
last_week AS (
    SELECT
        splitByChar(':', user)[2]   AS endpoint_id,
        count()                     AS calls_last_week
    FROM {query_log_table}
    WHERE event_date >= today() - {lookback_days} * 2
      AND event_date <  today() - {lookback_days}
      AND user LIKE 'queryEndpoint:%'
      AND type = 'QueryFinish'
    GROUP BY endpoint_id
),
purposes AS (
    SELECT
        splitByChar(':', user)[2]   AS endpoint_id,
        any(query)                  AS sample_query
    FROM {query_log_table}
    WHERE user LIKE 'queryEndpoint:%'
      AND type = 'QueryFinish'
    GROUP BY endpoint_id
)
SELECT
    coalesce(tw.endpoint_id, lw.endpoint_id)            AS endpoint_id,
    coalesce(tw.calls_this_week, 0)                     AS calls_this_week,
    coalesce(lw.calls_last_week, 0)                     AS calls_last_week,
    if(lw.calls_last_week > 0,
        round(
            (toInt64(tw.calls_this_week) - toInt64(lw.calls_last_week))
            / lw.calls_last_week * 100,
            1
        ),
        NULL
    )                                                   AS wow_change_pct,
    p.sample_query                                      AS query_text
FROM this_week AS tw
FULL OUTER JOIN last_week AS lw ON tw.endpoint_id = lw.endpoint_id
LEFT JOIN purposes AS p
       ON coalesce(tw.endpoint_id, lw.endpoint_id) = p.endpoint_id
ORDER BY coalesce(tw.calls_this_week, 0) DESC
