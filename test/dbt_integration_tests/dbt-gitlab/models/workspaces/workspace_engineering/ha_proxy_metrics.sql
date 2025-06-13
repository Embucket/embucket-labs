WITH source AS (

  SELECT *
  FROM {{ ref('thanos_total_haproxy_bytes_out') }}

),

report AS (

  SELECT
    metric_backend AS backend,
    metric_created_at AS recorded_at,
    CASE
      WHEN REGEXP_LIKE(metric_backend, '.*https_git$') THEN 'HTTPs Git Data Transfer'
      WHEN REGEXP_LIKE(metric_backend, '.*registry$') THEN 'Registry Data Transfer'
      WHEN REGEXP_LIKE(metric_backend, '.*api$') THEN 'API Data Transfer'
      WHEN REGEXP_LIKE(metric_backend, '.*web$') THEN 'Web Data Transfer'
      WHEN REGEXP_LIKE(metric_backend, '.*pages_https?$') THEN 'Pages Data Transfer'
      WHEN REGEXP_LIKE(metric_backend, '.*websockets$') THEN 'WebSockets Data Transfer'
      WHEN REGEXP_LIKE(metric_backend, '.*ssh$') THEN 'SSH Data Transfer'
      ELSE 'TBD'
    END AS backend_category,
    metric_value AS egress_bytes,
    metric_value / (1000 * 1000 * 1000) AS egress_gigabytes,
    metric_value / (1024 * 1024 * 1024) AS egress_gibibytes
  FROM source
  -- The first data loads did not include the backend aggregation.
  WHERE metric_backend IS NOT NULL

)

SELECT *
FROM report
