{{ config(
    materialized='table',
    )
}}

WITH haproxy_source AS (

  SELECT * FROM {{ ref('thanos_total_haproxy_bytes_out') }}
  WHERE (
    metric_backend = 'https_git'
    OR metric_backend = 'ssh'
  )

),


rps AS (

  SELECT *
  FROM {{ ref ('thanos_gitaly_requests') }}
  WHERE day >= '2024-01-16'

),

inter_zone AS (

  -- inter-zone usage
  SELECT
    date_day                           AS day,
    'inter-zone'                       AS usage_type,
    NULL                               AS metric_backend,
    SUM(usage_amount_in_pricing_units) AS usage_gib,
    SUM(net_cost)                      AS net_cost
  FROM {{ ref ('rpt_gcp_billing_pl_day_ext') }}
  WHERE gcp_project_id LIKE 'gitlab-gitaly-gprd-%'
    AND gcp_sku_description = 'Network Inter Zone Data Transfer Out'
    AND date_day >= '2024-01-16'
  GROUP BY 1, 2, 3
  ORDER BY 1 DESC

),

haproxy AS (

  -- https_git and ssh usage through HAproxy
  SELECT
    DATE(metric_created_at)                   AS day,
    'haproxy'                                 AS usage_type,
    metric_backend,
    SUM(metric_value) / POW(1024, 3)          AS usage_gib,
    SUM(metric_value) / POW(1024, 3) * 0.0328 AS net_cost
  FROM haproxy_source
  WHERE (DATE(metric_created_at), metric_created_at) IN
    (
      SELECT
        DATE(metric_created_at),
        MAX(metric_created_at)
      FROM haproxy_source
      GROUP BY DATE(metric_created_at)
    )
    AND DATE(metric_created_at) >= '2024-01-16'
  GROUP BY 1, 2, 3


),

usage_cte AS (

  SELECT * FROM inter_zone
  UNION ALL
  SELECT * FROM haproxy

),

rolling AS (

  SELECT
    day,
    usage_type,
    metric_backend,
    usage_gib                                                                                                            AS usage_gib,
    SUM(usage_gib) OVER (PARTITION BY usage_type, metric_backend ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS rolling_7d_usage_gib,
    SUM(usage_gib) OVER (PARTITION BY usage_type, metric_backend ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS rolling_28d_usage_gib,
    net_cost                                                                                                             AS net_cost,
    SUM(net_cost) OVER (PARTITION BY usage_type, metric_backend ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)   AS rolling_7d_net_cost,
    SUM(net_cost) OVER (PARTITION BY usage_type, metric_backend ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)  AS rolling_28d_net_cost
  FROM usage_cte

),

joined AS (

  SELECT
    rolling.day,
    rolling.usage_type,
    rolling.metric_backend,
    --usage
    rolling.usage_gib,
    rolling.rolling_7d_usage_gib,
    rolling.rolling_28d_usage_gib,
    --cost
    rolling.net_cost,
    rolling.rolling_7d_net_cost,
    rolling.rolling_28d_net_cost,
    --requests
    rps.requests                                             AS requests,
    rps.rolling_7d_requests,
    rps.rolling_28d_requests,
    -- usage per request
    rolling.usage_gib / rps.requests                         AS usage_per_request,
    rolling.rolling_7d_usage_gib / rps.rolling_7d_requests   AS rolling_7d_usage_per_requests,
    rolling.rolling_28d_usage_gib / rps.rolling_28d_requests AS rolling_28d_usage_per_requests,
    -- cost per request
    rolling.net_cost / rps.requests                          AS cost_per_request,
    rolling.rolling_7d_net_cost / rps.rolling_7d_requests    AS rolling_7d_cost_per_request,
    rolling.rolling_28d_net_cost / rps.rolling_28d_requests  AS rolling_28d_cost_per_request
  FROM rolling
  LEFT JOIN rps
    ON rolling.day = rps.day


)

SELECT * FROM joined
