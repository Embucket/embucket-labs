{{ simple_cte([
    ('rpt_arr_snapshot_combined_8th_calendar_day','rpt_arr_snapshot_combined_8th_calendar_day'),
    ('rpt_arr_snapshot_combined_5th_calendar_day','rpt_arr_snapshot_combined_5th_calendar_day')
]) }},

unioned AS (

  SELECT
    arr_month,
    is_arr_month_finalized,
    fiscal_quarter_name_fy,
    fiscal_year,
    subscription_start_month,
    subscription_end_month,
    dim_billing_account_id,
    sold_to_country,
    billing_account_name,
    billing_account_number,
    dim_crm_account_id,
    dim_charge_id,
    dim_parent_crm_account_id,
    parent_crm_account_name,
    parent_crm_account_billing_country,
    parent_crm_account_sales_segment,
    parent_crm_account_industry,
    parent_crm_account_geo,
    parent_crm_account_owner_team,
    parent_crm_account_sales_territory,
    dim_subscription_id,
    subscription_name,
    subscription_status,
    subscription_sales_type,
    product_name,
    product_name_grouped,
    product_rate_plan_name,
    product_rate_plan_charge_name,
    product_deployment_type,
    product_tier_name,
    product_delivery_type,
    product_ranking,
    service_type,
    unit_of_measure,
    mrr,
    arr,
    quantity,
    is_arpu,
    is_licensed_user,
    parent_account_cohort_month,
    months_since_parent_account_cohort_start,
    arr_band_calc,
    parent_crm_account_employee_count_band
  FROM rpt_arr_snapshot_combined_8th_calendar_day
  WHERE arr_month < '2024-03-01'

  UNION ALL

  SELECT
    arr_month,
    is_arr_month_finalized,
    fiscal_quarter_name_fy,
    fiscal_year,
    subscription_start_month,
    subscription_end_month,
    dim_billing_account_id,
    sold_to_country,
    billing_account_name,
    billing_account_number,
    dim_crm_account_id,
    dim_charge_id,
    dim_parent_crm_account_id,
    parent_crm_account_name,
    parent_crm_account_billing_country,
    parent_crm_account_sales_segment,
    parent_crm_account_industry,
    parent_crm_account_geo,
    parent_crm_account_owner_team,
    parent_crm_account_sales_territory,
    dim_subscription_id,
    subscription_name,
    subscription_status,
    subscription_sales_type,
    product_name,
    product_name_grouped,
    product_rate_plan_name,
    product_rate_plan_charge_name,
    product_deployment_type,
    product_tier_name,
    product_delivery_type,
    product_ranking,
    service_type,
    unit_of_measure,
    mrr,
    arr,
    quantity,
    is_arpu,
    is_licensed_user,
    parent_account_cohort_month,
    months_since_parent_account_cohort_start,
    arr_band_calc,
    parent_crm_account_employee_count_band
  FROM rpt_arr_snapshot_combined_5th_calendar_day
  WHERE arr_month >= '2024-03-01'

),

segment_ranking AS (
  SELECT
    *,
    (CASE WHEN parent_crm_account_sales_segment = 'Large' THEN 4
      WHEN parent_crm_account_sales_segment = 'Mid-Market' THEN 3
      WHEN parent_crm_account_sales_segment = 'SMB' THEN 2
      WHEN parent_crm_account_sales_segment = 'PubSec' THEN 1
    END) AS segment_ranking

  FROM unioned
),

segment_maxed AS (
  SELECT
    dim_parent_crm_account_id,
    MAX(segment_ranking) AS segment_ranked,
    (CASE WHEN segment_ranked = 4 THEN 'Large'
      WHEN segment_ranked = 3 THEN 'Mid-Market'
      WHEN segment_ranked = 2 THEN 'SMB'
      WHEN segment_ranked = 1 THEN 'PubSec'
    END)                 AS segment
  FROM segment_ranking
  GROUP BY 1
),

segment AS (
  SELECT
    unioned.*,
    segment_maxed.segment AS segment_modified
  FROM unioned
  LEFT JOIN segment_maxed
    ON unioned.dim_parent_crm_account_id = segment_maxed.dim_parent_crm_account_id
)

, final as (
SELECT
  *,
     CASE
        WHEN product_name ILIKE '%Premium%' THEN 'Premium'
        WHEN COALESCE(product_rate_plan_charge_name, product_rate_plan_name) ILIKE '%duo%' THEN 'Duo Pro'
        WHEN COALESCE(product_rate_plan_charge_name, product_rate_plan_name) ILIKE '%Success%Plan%Services%' THEN 'Success Plan Services'
        WHEN COALESCE(product_rate_plan_charge_name, product_rate_plan_name) ILIKE '%Dedicate%' THEN 'Dedicated-Ultimate'
        WHEN product_name ILIKE '%Ultimate%' THEN 'Ultimate'
        WHEN COALESCE(product_rate_plan_charge_name, product_rate_plan_name) ILIKE '%Enterprise%Agile%Planning%' THEN 'Enterprise Agile Planning'
        WHEN COALESCE(product_rate_plan_charge_name, product_rate_plan_name) ILIKE '%Gitlab%Storage%' THEN 'Storage'
        WHEN COALESCE(product_rate_plan_charge_name, product_rate_plan_name) ILIKE '%Assigned Support Engineer%' THEN 'Assigned Support Engineer'
        WHEN product_name ILIKE '%Bronze%'
        OR product_tier_name ILIKE '%Starter%' THEN 'Bronze/Starter'
        ELSE 'Others'
    END AS product_name_modified,
  CASE WHEN product_delivery_type = 'Not Applicable'
      THEN
        (CASE WHEN product_rate_plan_name LIKE 'SaaS%' THEN 'SaaS'
          WHEN product_rate_plan_name LIKE 'Self-Managed%' THEN 'Self-Managed'
          WHEN product_rate_plan_name LIKE 'Dedicated%' THEN 'SaaS'
        END)
    ELSE product_delivery_type END
    AS product_delivery_type_modified
FROM segment
)

SELECT *,
  MAX(arr_month) OVER () AS max_arr_month
FROM final

