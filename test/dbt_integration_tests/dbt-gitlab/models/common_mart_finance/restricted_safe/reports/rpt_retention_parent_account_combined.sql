{{ simple_cte([
 ('rpt_retention_parent_account', 'rpt_retention_parent_account'),
 ('rpt_retention_future_parent_account', 'rpt_retention_future_parent_account')
]) }},

unioned AS (

  SELECT
    primary_key,
    dim_parent_crm_account_id,
    parent_crm_account_name                  AS parent_crm_account_name_live,
    is_arr_month_finalized,
    retention_month,
    retention_fiscal_quarter_name_fy,
    retention_fiscal_year,
    retention_fiscal_quarter_name_fy         AS report_retention_fiscal_quarter_name_fy,
    retention_fiscal_year                    AS report_retention_fiscal_year,
    parent_crm_account_sales_segment         AS parent_crm_account_sales_segment_live,
    parent_crm_account_sales_segment_grouped AS parent_crm_account_sales_segment_grouped_live,
    parent_crm_account_geo                   AS parent_crm_account_geo_live,
    retention_arr_band,
    prior_year_arr,
    prior_year_mrr,
    net_retention_arr,
    net_retention_mrr,
    churn_arr,
    gross_retention_arr,
    prior_year_quantity,
    net_retention_quantity,
    net_retention_product_category,
    prior_year_product_category,
    net_retention_product_ranking,
    prior_year_product_ranking
  FROM rpt_retention_parent_account
  WHERE retention_month < '2023-08-01'

  UNION ALL

  SELECT
    primary_key,
    dim_parent_crm_account_id,
    parent_crm_account_name_live,
    is_arr_month_finalized,
    retention_month,
    retention_fiscal_quarter_name_fy,
    retention_fiscal_year,
    report_retention_fiscal_quarter_name_fy,
    report_retention_fiscal_year,
    parent_crm_account_sales_segment_live,
    parent_crm_account_sales_segment_grouped_live,
    parent_crm_account_geo_live,
    retention_arr_band,
    prior_year_arr,
    prior_year_mrr,
    net_retention_arr,
    net_retention_mrr,
    churn_arr,
    gross_retention_arr,
    prior_year_quantity,
    net_retention_quantity,
    net_retention_product_category,
    prior_year_product_category,
    net_retention_product_ranking,
    prior_year_product_ranking
  FROM rpt_retention_future_parent_account
  WHERE retention_month >= '2023-08-01'

)

, max_retention_month AS (
-- find the max month that actually has any retention data, because there are future months with no data in them yet.
  SELECT
    MAX(retention_month) AS max_retention_month
  FROM unioned
  WHERE net_retention_arr != 0

)

, final AS (

SELECT
  unioned.*,
  max_retention_month.max_retention_month
FROM unioned
  CROSS JOIN max_retention_month

)

{{ dbt_audit(
 cte_ref="final",
 created_by="@chrissharp",
 updated_by="@chrissharp",
 created_date="2023-11-08",
 updated_date="2023-11-08"
) }}
