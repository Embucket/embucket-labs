WITH daily_costs AS (

  SELECT
    DATE(date_day) AS day,
    SUM(net_cost)  AS net_cost
  FROM {{ ref('rpt_gcp_billing_pl_day_ext') }}
  WHERE DATE_TRUNC('month', DATE(date_day)) >= '2023-11-01'
    AND gcp_project_id IN ('unreview-poc-390200e5')
    AND gcp_service_description = 'Vertex AI'
  GROUP BY 1

)

SELECT
  d.day,
  d.net_cost                                                                      AS daily_net_cost,
  SUM(d.net_cost) OVER (ORDER BY d.day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS rolling_7d_net_cost,
  SUM(d.net_cost) OVER (ORDER BY d.day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS rolling_28d_net_cost
FROM daily_costs AS d
ORDER BY d.day DESC
