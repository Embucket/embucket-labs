{{ config(
    tags=["six_hourly"]
) }}

{{ simple_cte([
    ('rpt_gtm_crm_actuals','rpt_gtm_crm_actuals'),
    ('rpt_gtm_pivoted_targets', 'rpt_gtm_pivoted_targets'),
    ('dim_date','dim_date'),
    ('dim_crm_user_hierarchy', 'dim_crm_user_hierarchy'),
    ('dim_order_type', 'dim_order_type'),
    ('dim_sales_qualified_source', 'dim_sales_qualified_source')
]) }},

--scaffold

scaffold AS (

  SELECT
    rpt_gtm_crm_actuals.actual_date_id AS date_id,
    rpt_gtm_crm_actuals.dim_crm_current_account_set_hierarchy_sk,
    rpt_gtm_crm_actuals.dim_order_type_id,
    rpt_gtm_crm_actuals.dim_sales_qualified_source_id
  FROM rpt_gtm_crm_actuals

  UNION

  SELECT
    rpt_gtm_pivoted_targets.target_date_id,
    rpt_gtm_pivoted_targets.dim_crm_user_hierarchy_sk,
    rpt_gtm_pivoted_targets.dim_order_type_id,
    rpt_gtm_pivoted_targets.dim_sales_qualified_source_id
  FROM rpt_gtm_pivoted_targets

),

base AS (
  SELECT
    scaffold.date_id,
    scaffold.dim_crm_current_account_set_hierarchy_sk,
    scaffold.dim_order_type_id,
    scaffold.dim_sales_qualified_source_id,
    dim_crm_user_hierarchy.crm_user_geo,
    dim_crm_user_hierarchy.crm_user_region,
    dim_crm_user_hierarchy.crm_user_area,
    dim_crm_user_hierarchy.crm_user_business_unit,
    dim_crm_user_hierarchy.crm_user_sales_segment,
    dim_crm_user_hierarchy.crm_user_role_level_1,
    dim_crm_user_hierarchy.crm_user_role_level_2,
    dim_crm_user_hierarchy.crm_user_role_level_3,
    dim_crm_user_hierarchy.crm_user_role_level_4,
    dim_crm_user_hierarchy.crm_user_role_level_5,
    dim_crm_user_hierarchy.crm_user_role_name,
    dim_order_type.order_type_grouped,
    dim_order_type.order_type_name,
    dim_sales_qualified_source.sales_qualified_source_name,
    dim_sales_qualified_source.sales_qualified_source_grouped,
    dim_date.fiscal_quarter_name_fy,
    dim_date.date_actual,
    dim_date.day_of_fiscal_quarter,
    dim_date.day_of_month,
    dim_date.day_of_fiscal_year,
    dim_date.day_of_year,
    dim_date.first_day_of_month,
    dim_date.first_day_of_fiscal_quarter,
    dim_date.last_day_of_fiscal_quarter,
    dim_date.last_day_of_fiscal_year,
    dim_date.last_day_of_month,
    dim_date.first_day_of_fiscal_year,
    dim_date.fiscal_month_name,
    dim_date.fiscal_year,
    dim_date.first_day_of_year
  FROM scaffold
  INNER JOIN dim_crm_user_hierarchy
    ON scaffold.dim_crm_current_account_set_hierarchy_sk = dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk
  INNER JOIN dim_order_type
    ON scaffold.dim_order_type_id = dim_order_type.dim_order_type_id
  INNER JOIN dim_sales_qualified_source
    ON scaffold.dim_sales_qualified_source_id = dim_sales_qualified_source.dim_sales_qualified_source_id
  FULL OUTER JOIN dim_date
    ON scaffold.date_id = dim_date.date_id
)

SELECT *
FROM base
