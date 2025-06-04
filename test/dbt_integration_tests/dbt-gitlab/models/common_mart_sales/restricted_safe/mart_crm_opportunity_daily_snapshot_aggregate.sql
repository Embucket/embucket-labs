{{ config(
    materialized="incremental",
    unique_key="opportunity_daily_snapshot_aggregate_pk"
) }}

WITH fct_crm_opportunity AS (
  
  SELECT *
  FROM {{ ref('fct_crm_opportunity_daily_snapshot_aggregate') }}

  {% if is_incremental() %}
    WHERE snapshot_date > (SELECT MAX(snapshot_date) FROM {{this}})
  {% endif %}

), dim_date AS (

  SELECT *
  FROM {{ ref('dim_date') }}

),

dim_crm_user_hierarchy AS (

  SELECT *
  FROM {{ ref('dim_crm_user_hierarchy') }}

),

distinct_quarters AS (

  SELECT DISTINCT
    first_day_of_fiscal_quarter,
    fiscal_quarter_name_fy,
    fiscal_year
  FROM dim_date

),

final AS (

  SELECT
    fct_crm_opportunity.opportunity_daily_snapshot_aggregate_pk,
    fct_crm_opportunity.dim_crm_current_account_set_hierarchy_sk,

    dim_crm_user_hierarchy.crm_user_sales_segment                                                         AS report_segment,
    dim_crm_user_hierarchy.crm_user_geo                                                                   AS report_geo,
    dim_crm_user_hierarchy.crm_user_region                                                                AS report_region,
    dim_crm_user_hierarchy.crm_user_area                                                                  AS report_area,
    dim_crm_user_hierarchy.crm_user_business_unit                                                         AS report_business_unit,
    dim_crm_user_hierarchy.crm_user_role_name                                                             AS report_role_name,
    dim_crm_user_hierarchy.crm_user_role_level_1                                                          AS report_role_level_1,
    dim_crm_user_hierarchy.crm_user_role_level_2                                                          AS report_role_level_2,
    dim_crm_user_hierarchy.crm_user_role_level_3                                                          AS report_role_level_3,
    dim_crm_user_hierarchy.crm_user_role_level_4                                                          AS report_role_level_4,
    dim_crm_user_hierarchy.crm_user_role_level_5                                                          AS report_role_level_5,

    --snapshot fields
    fct_crm_opportunity.sales_qualified_source,
    fct_crm_opportunity.sales_qualified_source_grouped,
    fct_crm_opportunity.order_type,
    fct_crm_opportunity.order_type_grouped,
    fct_crm_opportunity.stage_name,
    fct_crm_opportunity.deal_path,
    fct_crm_opportunity.subscription_type,
    fct_crm_opportunity.is_stage_1_plus,
    fct_crm_opportunity.is_stage_3_plus,
    fct_crm_opportunity.is_stage_4_plus,
    
    --live fields
    fct_crm_opportunity.sales_qualified_source_live,
    fct_crm_opportunity.sales_qualified_source_grouped_live,
    fct_crm_opportunity.order_type_live,
    fct_crm_opportunity.order_type_grouped_live,
    fct_crm_opportunity.deal_path_live,

    fct_crm_opportunity.snapshot_date,
    fct_crm_opportunity.snapshot_month,
    fct_crm_opportunity.snapshot_fiscal_year,
    fct_crm_opportunity.snapshot_fiscal_quarter_name,
    fct_crm_opportunity.snapshot_fiscal_quarter_date,
    fct_crm_opportunity.snapshot_day_of_fiscal_quarter_normalised,
    fct_crm_opportunity.snapshot_day_of_fiscal_year_normalised,
    fct_crm_opportunity.landing_quarter_relative_to_arr_created_date,
    fct_crm_opportunity.landing_quarter_relative_to_snapshot_date,
    fct_crm_opportunity.snapshot_to_close_diff,
    fct_crm_opportunity.arr_created_to_close_diff,

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
    close_date.first_day_of_fiscal_quarter                                                                AS close_fiscal_quarter_date,
    close_date.fiscal_quarter_name_fy                                                                     AS close_fiscal_quarter_name,
    close_date.fiscal_year                                                                                AS close_fiscal_year,
    fct_crm_opportunity.close_fiscal_quarter_name_for_pipegen_grouping,
    created_date.first_day_of_fiscal_quarter                                                              AS created_fiscal_quarter_date,
    created_date.fiscal_quarter_name_fy                                                                   AS created_fiscal_quarter_name,
    created_date.fiscal_year                                                                              AS created_fiscal_year,
    arr_created_date.first_day_of_fiscal_quarter                                                          AS arr_created_fiscal_quarter_date,
    arr_created_date.fiscal_quarter_name_fy                                                               AS arr_created_fiscal_quarter_name,
    arr_created_date.fiscal_year                                                                          AS arr_created_fiscal_year,
    dim_date.date_day                                                                                     AS snapshot_day,
    dim_date.day_name                                                                                     AS snapshot_day_name,
    dim_date.day_of_week                                                                                  AS snapshot_day_of_week,
    dim_date.first_day_of_week                                                                            AS snapshot_first_day_of_week,
    dim_date.week_of_year                                                                                 AS snapshot_week_of_year,
    dim_date.day_of_month                                                                                 AS snapshot_day_of_month,
    dim_date.day_of_quarter                                                                               AS snapshot_day_of_quarter,
    dim_date.day_of_year                                                                                  AS snapshot_day_of_year,
    dim_date.fiscal_quarter                                                                               AS snapshot_fiscal_quarter,
    dim_date.day_of_fiscal_quarter                                                                        AS snapshot_day_of_fiscal_quarter,
    dim_date.day_of_fiscal_year                                                                           AS snapshot_day_of_fiscal_year,
    dim_date.month_name                                                                                   AS snapshot_month_name,
    dim_date.first_day_of_month                                                                           AS snapshot_first_day_of_month,
    dim_date.last_day_of_month                                                                            AS snapshot_last_day_of_month,
    dim_date.first_day_of_year                                                                            AS snapshot_first_day_of_year,
    dim_date.last_day_of_year                                                                             AS snapshot_last_day_of_year,
    dim_date.first_day_of_quarter                                                                         AS snapshot_first_day_of_quarter,
    dim_date.last_day_of_quarter                                                                          AS snapshot_last_day_of_quarter,
    dim_date.first_day_of_fiscal_quarter                                                                  AS snapshot_first_day_of_fiscal_quarter,
    dim_date.last_day_of_fiscal_quarter                                                                   AS snapshot_last_day_of_fiscal_quarter,
    dim_date.first_day_of_fiscal_year                                                                     AS snapshot_first_day_of_fiscal_year,
    dim_date.last_day_of_fiscal_year                                                                      AS snapshot_last_day_of_fiscal_year,
    dim_date.week_of_fiscal_year                                                                          AS snapshot_week_of_fiscal_year,
    dim_date.month_of_fiscal_year                                                                         AS snapshot_month_of_fiscal_year,
    dim_date.last_day_of_week                                                                             AS snapshot_last_day_of_week,
    dim_date.quarter_name                                                                                 AS snapshot_quarter_name,
    dim_date.fiscal_quarter_name_fy                                                                       AS snapshot_fiscal_quarter_name_fy,
    dim_date.fiscal_quarter_number_absolute                                                               AS snapshot_fiscal_quarter_number_absolute,
    dim_date.fiscal_month_name                                                                            AS snapshot_fiscal_month_name,
    dim_date.fiscal_month_name_fy                                                                         AS snapshot_fiscal_month_name_fy,
    dim_date.holiday_desc                                                                                 AS snapshot_holiday_desc,
    dim_date.is_holiday                                                                                   AS snapshot_is_holiday,
    dim_date.last_month_of_fiscal_quarter                                                                 AS snapshot_last_month_of_fiscal_quarter,
    dim_date.is_first_day_of_last_month_of_fiscal_quarter                                                 AS snapshot_is_first_day_of_last_month_of_fiscal_quarter,
    dim_date.last_month_of_fiscal_year                                                                    AS snapshot_last_month_of_fiscal_year,
    dim_date.is_first_day_of_last_month_of_fiscal_year                                                    AS snapshot_is_first_day_of_last_month_of_fiscal_year,
    dim_date.days_in_month_count                                                                          AS snapshot_days_in_month_count,
    dim_date.week_of_month_normalised                                                                     AS snapshot_week_of_month_normalised,
    dim_date.week_of_fiscal_quarter_normalised                                                            AS snapshot_week_of_fiscal_quarter_normalised,
    dim_date.is_first_day_of_fiscal_quarter_week                                                          AS snapshot_is_first_day_of_fiscal_quarter_week,
    dim_date.days_until_last_day_of_month                                                                 AS snapshot_days_until_last_day_of_month,
    dim_date.week_of_fiscal_quarter                                                                       AS snapshot_week_of_fiscal_quarter,

    --additive fields
    fct_crm_opportunity.created_arr_in_snapshot_quarter,
    fct_crm_opportunity.closed_won_opps_in_snapshot_quarter,
    fct_crm_opportunity.closed_opps_in_snapshot_quarter,
    fct_crm_opportunity.booked_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.created_deals_in_snapshot_quarter,
    fct_crm_opportunity.cycle_time_in_days_in_snapshot_quarter,
    fct_crm_opportunity.booked_deal_count_in_snapshot_quarter,
    fct_crm_opportunity.open_1plus_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.open_3plus_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.open_4plus_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.open_1plus_deal_count_in_snapshot_quarter,
    fct_crm_opportunity.open_3plus_deal_count_in_snapshot_quarter,
    fct_crm_opportunity.open_4plus_deal_count_in_snapshot_quarter,
    fct_crm_opportunity.positive_booked_deal_count_in_snapshot_quarter,
    fct_crm_opportunity.positive_booked_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.positive_open_deal_count_in_snapshot_quarter,
    fct_crm_opportunity.positive_open_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.closed_deals_in_snapshot_quarter,
    fct_crm_opportunity.closed_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.net_incremental_acv,
    fct_crm_opportunity.incremental_acv,
    fct_crm_opportunity.created_in_snapshot_quarter_net_arr,
    fct_crm_opportunity.created_in_snapshot_quarter_deal_count,
    fct_crm_opportunity.opportunity_based_iacv_to_net_arr_ratio,
    fct_crm_opportunity.segment_order_type_iacv_to_net_arr_ratio,
    fct_crm_opportunity.calculated_from_ratio_net_arr,
    fct_crm_opportunity.net_arr,
    fct_crm_opportunity.raw_net_arr,
    fct_crm_opportunity.created_and_won_same_quarter_net_arr,
    fct_crm_opportunity.new_logo_count,
    fct_crm_opportunity.amount,
    fct_crm_opportunity.recurring_amount,
    fct_crm_opportunity.true_up_amount,
    fct_crm_opportunity.proserv_amount,
    fct_crm_opportunity.other_non_recurring_amount,
    fct_crm_opportunity.arr_basis,
    fct_crm_opportunity.arr,
    fct_crm_opportunity.count_crm_attribution_touchpoints,
    fct_crm_opportunity.weighted_linear_iacv,
    fct_crm_opportunity.count_campaigns,
    fct_crm_opportunity.probability,
    fct_crm_opportunity.days_in_sao,
    fct_crm_opportunity.open_1plus_deal_count,
    fct_crm_opportunity.open_3plus_deal_count,
    fct_crm_opportunity.open_4plus_deal_count,
    fct_crm_opportunity.booked_deal_count,
    fct_crm_opportunity.churned_contraction_deal_count,
    fct_crm_opportunity.open_1plus_net_arr,
    fct_crm_opportunity.open_3plus_net_arr,
    fct_crm_opportunity.open_4plus_net_arr,
    fct_crm_opportunity.booked_net_arr,
    fct_crm_opportunity.churned_contraction_net_arr,
    fct_crm_opportunity.calculated_deal_count,
    fct_crm_opportunity.cycle_time_in_days,
    fct_crm_opportunity.booked_churned_contraction_deal_count,
    fct_crm_opportunity.booked_churned_contraction_net_arr,
    fct_crm_opportunity.renewal_amount,
    fct_crm_opportunity.total_contract_value,
    fct_crm_opportunity.days_in_stage,
    fct_crm_opportunity.calculated_age_in_days,
    fct_crm_opportunity.days_since_last_activity,
    fct_crm_opportunity.won_arr_basis_for_clari,
    fct_crm_opportunity.arr_basis_for_clari,
    fct_crm_opportunity.forecasted_churn_for_clari,
    fct_crm_opportunity.override_arr_basis_clari,
    fct_crm_opportunity.vsa_start_date_net_arr,
    fct_crm_opportunity.created_arr,
    fct_crm_opportunity.closed_won_opps,
    fct_crm_opportunity.closed_opps,
    fct_crm_opportunity.created_deals,
    fct_crm_opportunity.positive_booked_deal_count,
    fct_crm_opportunity.positive_booked_net_arr,
    fct_crm_opportunity.positive_open_deal_count,
    fct_crm_opportunity.positive_open_net_arr,
    fct_crm_opportunity.closed_deals,
    fct_crm_opportunity.closed_net_arr,
    'aggregate'                                                                                           AS source,
    IFF(dim_date.current_first_day_of_fiscal_quarter = snapshot_first_day_of_fiscal_quarter, TRUE, FALSE) AS is_current_snapshot_quarter,
    IFF(dim_date.current_first_day_of_week = dim_date.first_day_of_week, TRUE, FALSE)                     AS is_current_snapshot_week
  FROM fct_crm_opportunity
  INNER JOIN dim_date
    ON fct_crm_opportunity.snapshot_date = dim_date.date_actual
  LEFT JOIN distinct_quarters AS close_date
    ON fct_crm_opportunity.close_fiscal_quarter_date = close_date.first_day_of_fiscal_quarter
  LEFT JOIN distinct_quarters AS arr_created_date
    ON fct_crm_opportunity.arr_created_fiscal_quarter_date = arr_created_date.first_day_of_fiscal_quarter
  LEFT JOIN distinct_quarters AS created_date
    ON fct_crm_opportunity.created_fiscal_quarter_date = created_date.first_day_of_fiscal_quarter
  LEFT JOIN dim_crm_user_hierarchy
    ON fct_crm_opportunity.dim_crm_current_account_set_hierarchy_sk = dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk

)

SELECT *
FROM final
