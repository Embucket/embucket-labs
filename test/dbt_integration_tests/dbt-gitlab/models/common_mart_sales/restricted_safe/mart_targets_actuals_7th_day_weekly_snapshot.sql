WITH targets_actuals AS (

  SELECT *
  FROM {{ ref('fct_targets_actuals_7th_day_weekly_snapshot') }}

),

dim_date AS (

  SELECT *
  FROM {{ ref('dim_date') }}

),

dim_crm_user_hierarchy AS (

  SELECT *
  FROM {{ ref('dim_crm_user_hierarchy') }}

),

sales_qualified_source AS (

  SELECT *
  FROM {{ ref('dim_sales_qualified_source') }}

),

order_type AS (

  SELECT *
  FROM {{ ref('dim_order_type') }}

),

final AS (

  SELECT
    targets_actuals.*,
    

    --fields to deprecate
    dim_crm_user_hierarchy.crm_user_sales_segment         AS crm_current_account_set_sales_segment,
    dim_crm_user_hierarchy.crm_user_geo                   AS crm_current_account_set_geo,
    dim_crm_user_hierarchy.crm_user_region                AS crm_current_account_set_region,
    dim_crm_user_hierarchy.crm_user_area                  AS crm_current_account_set_area,
    dim_crm_user_hierarchy.crm_user_business_unit         AS crm_current_account_set_business_unit,
    dim_crm_user_hierarchy.crm_user_role_name             AS crm_current_account_set_role_name,
    dim_crm_user_hierarchy.crm_user_role_level_1          AS crm_current_account_set_role_level_1,
    dim_crm_user_hierarchy.crm_user_role_level_2          AS crm_current_account_set_role_level_2,
    dim_crm_user_hierarchy.crm_user_role_level_3          AS crm_current_account_set_role_level_3,
    dim_crm_user_hierarchy.crm_user_role_level_4          AS crm_current_account_set_role_level_4,
    dim_crm_user_hierarchy.crm_user_role_level_5          AS crm_current_account_set_role_level_5,
    
    dim_crm_user_hierarchy.crm_user_sales_segment         AS report_segment,
    dim_crm_user_hierarchy.crm_user_geo                   AS report_geo,
    dim_crm_user_hierarchy.crm_user_region                AS report_region,
    dim_crm_user_hierarchy.crm_user_area                  AS report_area,
    dim_crm_user_hierarchy.crm_user_business_unit         AS report_business_unit,
    dim_crm_user_hierarchy.crm_user_role_name             AS report_role_name,
    dim_crm_user_hierarchy.crm_user_role_level_1          AS report_role_level_1,
    dim_crm_user_hierarchy.crm_user_role_level_2          AS report_role_level_2,
    dim_crm_user_hierarchy.crm_user_role_level_3          AS report_role_level_3,
    dim_crm_user_hierarchy.crm_user_role_level_4          AS report_role_level_4,
    dim_crm_user_hierarchy.crm_user_role_level_5          AS report_role_level_5,

    -- Dates
    dim_date.current_day_name,
    dim_date.current_date_actual,
    dim_date.current_fiscal_year,
    dim_date.current_first_day_of_fiscal_year,
    dim_date.current_fiscal_quarter_name_fy,
    dim_date.current_first_day_of_month,
    dim_date.current_first_day_of_fiscal_quarter,
    dim_date.current_day_of_month,
    dim_date.current_day_of_fiscal_quarter,
    dim_date.current_day_of_fiscal_year,
    dim_date.current_first_day_of_week,
    dim_date.current_week_of_fiscal_quarter_normalised,
    dim_date.current_week_of_fiscal_quarter,
    dim_date.date_day                                     AS snapshot_day,
    dim_date.date_actual                                  AS snapshot_date,
    dim_date.day_name                                     AS snapshot_day_name,
    dim_date.day_of_week                                  AS snapshot_day_of_week,
    dim_date.first_day_of_week                            AS snapshot_first_day_of_week,
    dim_date.week_of_year                                 AS snapshot_week_of_year,
    dim_date.day_of_month                                 AS snapshot_day_of_month,
    dim_date.day_of_quarter                               AS snapshot_day_of_quarter,
    dim_date.day_of_year                                  AS snapshot_day_of_year,
    dim_date.fiscal_quarter                               AS snapshot_fiscal_quarter,
    dim_date.fiscal_year                                  AS snapshot_fiscal_year,
    dim_date.day_of_fiscal_quarter                        AS snapshot_day_of_fiscal_quarter,
    dim_date.day_of_fiscal_year                           AS snapshot_day_of_fiscal_year,
    dim_date.first_day_of_month                           AS snapshot_month,
    dim_date.month_name                                   AS snapshot_month_name,
    dim_date.first_day_of_month                           AS snapshot_first_day_of_month,
    dim_date.last_day_of_month                            AS snapshot_last_day_of_month,
    dim_date.first_day_of_year                            AS snapshot_first_day_of_year,
    dim_date.last_day_of_year                             AS snapshot_last_day_of_year,
    dim_date.first_day_of_quarter                         AS snapshot_first_day_of_quarter,
    dim_date.last_day_of_quarter                          AS snapshot_last_day_of_quarter,
    dim_date.first_day_of_fiscal_quarter                  AS snapshot_first_day_of_fiscal_quarter,
    dim_date.last_day_of_fiscal_quarter                   AS snapshot_last_day_of_fiscal_quarter,
    dim_date.first_day_of_fiscal_year                     AS snapshot_first_day_of_fiscal_year,
    dim_date.last_day_of_fiscal_year                      AS snapshot_last_day_of_fiscal_year,
    dim_date.week_of_fiscal_year                          AS snapshot_week_of_fiscal_year,
    dim_date.month_of_fiscal_year                         AS snapshot_month_of_fiscal_year,
    dim_date.last_day_of_week                             AS snapshot_last_day_of_week,
    dim_date.quarter_name                                 AS snapshot_quarter_name,
    dim_date.fiscal_quarter_name_fy                       AS snapshot_fiscal_quarter_name,
    dim_date.fiscal_quarter_name_fy                       AS snapshot_fiscal_quarter_name_fy,
    dim_date.first_day_of_fiscal_quarter                  AS snapshot_fiscal_quarter_date,
    dim_date.fiscal_quarter_number_absolute               AS snapshot_fiscal_quarter_number_absolute,
    dim_date.fiscal_month_name                            AS snapshot_fiscal_month_name,
    dim_date.fiscal_month_name_fy                         AS snapshot_fiscal_month_name_fy,
    dim_date.holiday_desc                                 AS snapshot_holiday_desc,
    dim_date.is_holiday                                   AS snapshot_is_holiday,
    dim_date.last_month_of_fiscal_quarter                 AS snapshot_last_month_of_fiscal_quarter,
    dim_date.is_first_day_of_last_month_of_fiscal_quarter AS snapshot_is_first_day_of_last_month_of_fiscal_quarter,
    dim_date.last_month_of_fiscal_year                    AS snapshot_last_month_of_fiscal_year,
    dim_date.is_first_day_of_last_month_of_fiscal_year    AS snapshot_is_first_day_of_last_month_of_fiscal_year,
    dim_date.days_in_month_count                          AS snapshot_days_in_month_count,
    dim_date.week_of_month_normalised                     AS snapshot_week_of_month_normalised,
    dim_date.week_of_fiscal_quarter_normalised            AS snapshot_week_of_fiscal_quarter_normalised,
    dim_date.is_first_day_of_fiscal_quarter_week          AS snapshot_is_first_day_of_fiscal_quarter_week,
    dim_date.days_until_last_day_of_month                 AS snapshot_days_until_last_day_of_month,
    dim_date.week_of_fiscal_quarter                       AS snapshot_week_of_fiscal_quarter,
    dim_date.day_of_fiscal_quarter_normalised             AS snapshot_day_of_fiscal_quarter_normalised,
    dim_date.day_of_fiscal_year_normalised                AS snapshot_day_of_fiscal_year_normalised,
    sales_qualified_source.sales_qualified_source_name,
    sales_qualified_source.sales_qualified_source_grouped,
    order_type.order_type_name                            AS order_type,
    order_type.order_type_grouped,
    'targets_actuals'                                     AS source
  FROM targets_actuals
  LEFT JOIN dim_date
    ON targets_actuals.date_actual = dim_date.date_actual
  LEFT JOIN dim_crm_user_hierarchy
    ON targets_actuals.dim_crm_current_account_set_hierarchy_sk = dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk
  LEFT JOIN sales_qualified_source
    ON targets_actuals.dim_sales_qualified_source_id = sales_qualified_source.dim_sales_qualified_source_id
  LEFT JOIN order_type
    ON targets_actuals.dim_order_type_id = order_type.dim_order_type_id

)

SELECT *
FROM final
