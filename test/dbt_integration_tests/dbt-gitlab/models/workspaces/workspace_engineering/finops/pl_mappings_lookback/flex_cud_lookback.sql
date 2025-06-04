SELECT
  date_day,
  pl_category,
  SUM(net_cost)                                               AS cost,
  RATIO_TO_REPORT(SUM(net_cost)) OVER (PARTITION BY date_day) AS pl_percent
FROM {{ ref('rpt_gcp_billing_pl_day_combined') }}
WHERE gcp_service_description = 'Compute Engine'
  AND (LOWER(gcp_sku_description) LIKE '%ram%'
    OR LOWER(gcp_sku_description) LIKE '%core%'
    AND LOWER(gcp_sku_description) NOT LIKE '%commitment%'
    AND LOWER(gcp_sku_description) NOT LIKE '%t2d%')
  AND (usage_unit = 'seconds' OR usage_unit = 'bytes-seconds')
  AND pl_category IS NOT NULL
  AND DATE_TRUNC('month', date_day) >= '2023-02-01'
GROUP BY 1, 2
ORDER BY date_day DESC
