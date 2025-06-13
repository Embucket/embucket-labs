{{ simple_cte([
    ('mart_arr', 'mart_arr'),
    ('mart_product_usage_paid_user_metrics_monthly', 'mart_product_usage_paid_user_metrics_monthly')
    ])

}},

monthly_mart AS (
  SELECT
    --DISTINCT
    --  snapshot_month,
    dim_subscription_id_original,
    instance_type,
    subscription_status,
    subscription_start_date,
    subscription_end_date,
    term_end_date,
    MAX(DATE_TRUNC('month', ping_created_at)) AS latest_ping,
    MAX(license_user_count)                   AS license_user_count,
    MAX(billable_user_count)

      AS max_billable_user_count

  FROM mart_product_usage_paid_user_metrics_monthly
  WHERE instance_type = 'Production'
    AND subscription_status = 'Active'
  GROUP BY 1, 2, 3, 4, 5, 6
  --   and (snapshot_month = DATE_TRUNC('month',CURRENT_DATE) or snapshot_month = dateadd('month',-1,DATE_TRUNC('month',CURRENT_DATE)))
  HAVING (latest_ping = DATE_TRUNC('month', CURRENT_DATE) OR latest_ping = DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE)))
  ORDER BY 1
),

arr_data AS (
  SELECT
    mart_arr.* EXCLUDE (created_by, updated_by, model_created_date, model_updated_date, dbt_updated_at, dbt_created_at),
    --,monthly_mart.snapshot_month 
    COALESCE (mart_arr.contract_seat_reconciliation = 'Yes' AND mart_arr.turn_on_seat_reconciliation = 'Yes', FALSE)          AS qsr_enabled_flag,
    monthly_mart.max_billable_user_count,
    monthly_mart.license_user_count,
    monthly_mart.subscription_start_date,
    monthly_mart.subscription_end_date,
    monthly_mart.term_end_date,
    monthly_mart.max_billable_user_count - monthly_mart.license_user_count                                                    AS overage_count,
    (mart_arr.arr / NULLIFZERO(mart_arr.quantity)) * (monthly_mart.max_billable_user_count - monthly_mart.license_user_count) AS overage_amount
  --,max(snapshot_month) over(partition by monthly_mart.DIM_SUBSCRIPTION_ID_ORIGINAL) as latest_overage_month
  FROM mart_arr
  LEFT JOIN monthly_mart
    ON mart_arr.dim_subscription_id_original = monthly_mart.dim_subscription_id_original
  WHERE mart_arr.arr_month = DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE))
    AND mart_arr.product_tier_name LIKE '%Premium%'
),

output AS (
  SELECT
    *,
    --,monthly_mart.max_BILLABLE_USER_COUNT - monthly_mart.LICENSE_USER_COUNT AS overage_count
    (arr / NULLIFZERO(quantity)) AS arr_per_user,
    NULLIFZERO(quantity)         AS quantity_on_subscription,
    GREATEST(NULLIFZERO(max_billable_user_count), quantity_on_subscription) AS current_user_count,
    SUM(arr) OVER(PARTITION BY subscription_name) / NULLIFZERO(current_user_count) AS blended_price_per_user,
    arr_per_user / 12                                                                                                                                          AS monthly_price_per_user,
    COALESCE (monthly_price_per_user < (CASE WHEN subscription_end_month > '2024-04-01' THEN 29 ELSE 23.78 END) AND product_tier_name LIKE '%Premium%', FALSE) AS fy25_premium_price_increase_flag,
    CASE WHEN fy25_premium_price_increase_flag THEN (
        (CASE WHEN subscription_end_month > '2024-04-01' THEN 29 ELSE 23.78 END)
        - monthly_price_per_user
      ) * current_user_count * 12 ELSE 0
    END                                                                                                                                                        AS fy25_estimated_price_increase_impact,
    COALESCE (arr + fy25_estimated_price_increase_impact >= 7000 AND arr < 7000, FALSE)                                                                        AS fy25_likely_price_increase_uptier_flag,
    arr                                                                                                                                                        AS test_arr,
    current_user_count * (CASE WHEN subscription_end_month > '2024-04-01' THEN 29 ELSE 23.78 END) * 12                                                         AS next_arr
  FROM arr_data
  WHERE
    arr > 0
)

{{ dbt_audit(
    cte_ref="output",
    created_by="@mfleisher",
    updated_by="@mfleisher",
    created_date="2024-07-15",
    updated_date="2025-02-19"
) }}
