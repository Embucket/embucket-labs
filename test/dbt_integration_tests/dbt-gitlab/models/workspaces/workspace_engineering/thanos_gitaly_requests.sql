WITH source AS (

  SELECT *
  FROM {{ ref('thanos_gitaly_rps') }}
  WHERE is_success = TRUE
),

transformed AS (

  SELECT
    DATE_TRUNC('day', metric_created_at) AS day,
    SUM(metric_value) * 3600             AS requests
  FROM source
  GROUP BY 1
),

int AS (
  SELECT
    day           AS day,
    SUM(requests) AS requests
  FROM transformed
  GROUP BY day
)

SELECT
  day,
  requests,
  SUM(requests) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS rolling_7d_requests,
  SUM(requests) OVER (ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS rolling_28d_requests
FROM int
