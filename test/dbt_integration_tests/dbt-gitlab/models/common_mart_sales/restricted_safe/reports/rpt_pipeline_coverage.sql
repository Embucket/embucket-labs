{{ simple_cte([
    ('targets', 'mart_sales_funnel_target_daily'),
    ('actuals', 'mart_crm_opportunity_7th_day_weekly_snapshot'),
    ('dim_date', 'dim_date'),
    ('hierarchy', 'dim_crm_user_hierarchy')
    ])

}},

live_actuals AS (

  /* 
    Grab final numbers for the quarter from the live data to ensure
    we capture deals that are closed on the last day of the quarter
  */
  SELECT *
  FROM {{ ref('mart_crm_opportunity') }} AS mart_crm_opportunity
  INNER JOIN dim_date AS close_date
    ON mart_crm_opportunity.close_date = close_date.date_actual
  WHERE close_fiscal_quarter_date < current_first_day_of_fiscal_quarter

),

spine AS (

  {{ date_spine_7th_day() }}

),

total_targets AS (

  SELECT
    targets.fiscal_quarter_name_fy,
    targets.order_type_name,
    targets.order_type_grouped,
    targets.sales_qualified_source_name,
    targets.sales_qualified_source_grouped,
    targets.dim_crm_user_hierarchy_sk,
    SUM(CASE WHEN kpi_name = 'Net ARR' THEN daily_allocated_target END)                  AS net_arr_total_quarter_target,
    SUM(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN daily_allocated_target END) AS pipeline_created_total_quarter_target
  FROM targets
  {{ dbt_utils.group_by(n=6) }}

),

daily_actuals AS (

  SELECT
    actuals.snapshot_fiscal_quarter_name,
    actuals.snapshot_date,
    actuals.sales_qualified_source_live,
    actuals.sales_qualified_source_grouped_live,
    actuals.order_type_live,
    actuals.order_type_grouped_live,
    actuals.dim_crm_current_account_set_hierarchy_sk,
    SUM(booked_net_arr_in_snapshot_quarter)     AS booked_net_arr_in_snapshot_quarter,
    SUM(open_1plus_net_arr_in_snapshot_quarter) AS open_1plus_net_arr_in_snapshot_quarter,
    SUM(open_3plus_net_arr_in_snapshot_quarter) AS open_3plus_net_arr_in_snapshot_quarter,
    SUM(open_4plus_net_arr_in_snapshot_quarter) AS open_4plus_net_arr_in_snapshot_quarter
  FROM actuals
  {{ dbt_utils.group_by(n=7) }}
),

quarterly_actuals AS (

  SELECT
    live_actuals.close_fiscal_quarter_name,
    live_actuals.close_fiscal_quarter_date,
    live_actuals.sales_qualified_source_name,
    live_actuals.sales_qualified_source_grouped,
    live_actuals.order_type,
    live_actuals.order_type_grouped,
    live_actuals.dim_crm_current_account_set_hierarchy_sk,
    SUM(live_actuals.booked_net_arr) AS total_booked_net_arr
  FROM live_actuals
  {{ dbt_utils.group_by(n=7) }}


),

combined_data AS (

  SELECT
    dim_crm_current_account_set_hierarchy_sk,
    sales_qualified_source_live,
    sales_qualified_source_grouped_live,
    order_type_live,
    order_type_grouped_live,
    snapshot_fiscal_quarter_name
  FROM daily_actuals

  UNION 

  SELECT
    dim_crm_user_hierarchy_sk,
    sales_qualified_source_name,
    sales_qualified_source_grouped,
    order_type_name,
    order_type_grouped,
    fiscal_quarter_name_fy
  FROM total_targets

  UNION

  SELECT
    dim_crm_current_account_set_hierarchy_sk,
    sales_qualified_source_name,
    sales_qualified_source_grouped,
    order_type,
    order_type_grouped,
    close_fiscal_quarter_name
  FROM quarterly_actuals

),

base AS (

  /*
    Cross join all dimensions (hierarchy, qualified source, order type) and
    the dates to create a comprehensive set of all possible combinations of these dimensions and dates.
    This exhaustive combination is essential for scenarios where we need to account for all possible configurations in our analysis,
    ensuring that no combination is overlooked.

    When we eventually join this set of combinations with the quarterly actuals,
    it ensures that even the newly introduced dimensions are accounted for.
  */

  SELECT
    combined_data.dim_crm_current_account_set_hierarchy_sk,
    combined_data.sales_qualified_source_live,
    combined_data.sales_qualified_source_grouped_live,
    combined_data.order_type_live,
    combined_data.order_type_grouped_live,
    spine.day_7 AS snapshot_date,
    combined_data.snapshot_fiscal_quarter_name
  FROM combined_data
  INNER JOIN spine
    ON combined_data.snapshot_fiscal_quarter_name = spine.fiscal_quarter_name

),

final AS (

  SELECT
    base.dim_crm_current_account_set_hierarchy_sk,
    base.snapshot_date,
    base.sales_qualified_source_live,
    base.sales_qualified_source_grouped_live,
    base.order_type_live,
    base.order_type_grouped_live,

    --Hierarchy fields
    hierarchy.crm_user_sales_segment                                                         AS report_segment,
    hierarchy.crm_user_geo                                                                   AS report_geo,
    hierarchy.crm_user_region                                                                AS report_region,
    hierarchy.crm_user_area                                                                  AS report_area,
    hierarchy.crm_user_business_unit                                                         AS report_business_unit,
    hierarchy.crm_user_role_name                                                             AS report_role_name,
    hierarchy.crm_user_role_level_1                                                          AS report_role_level_1,
    hierarchy.crm_user_role_level_2                                                          AS report_role_level_2,
    hierarchy.crm_user_role_level_3                                                          AS report_role_level_3,
    hierarchy.crm_user_role_level_4                                                          AS report_role_level_4,
    hierarchy.crm_user_role_level_5                                                          AS report_role_level_5,

    --Dates
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
    dim_date.date_day                                                       AS snapshot_day,
    dim_date.day_name                                                       AS snapshot_day_name,
    dim_date.day_of_week                                                    AS snapshot_day_of_week,
    dim_date.first_day_of_week                                              AS snapshot_first_day_of_week,
    dim_date.week_of_year                                                   AS snapshot_week_of_year,
    dim_date.day_of_month                                                   AS snapshot_day_of_month,
    dim_date.day_of_quarter                                                 AS snapshot_day_of_quarter,
    dim_date.day_of_year                                                    AS snapshot_day_of_year,
    dim_date.fiscal_quarter                                                 AS snapshot_fiscal_quarter,
    dim_date.fiscal_year                                                    AS snapshot_fiscal_year,
    dim_date.day_of_fiscal_quarter                                          AS snapshot_day_of_fiscal_quarter,
    dim_date.day_of_fiscal_year                                             AS snapshot_day_of_fiscal_year,
    dim_date.first_day_of_month                                             AS snapshot_month,
    dim_date.month_name                                                     AS snapshot_month_name,
    dim_date.first_day_of_month                                             AS snapshot_first_day_of_month,
    dim_date.last_day_of_month                                              AS snapshot_last_day_of_month,
    dim_date.first_day_of_year                                              AS snapshot_first_day_of_year,
    dim_date.last_day_of_year                                               AS snapshot_last_day_of_year,
    dim_date.first_day_of_quarter                                           AS snapshot_first_day_of_quarter,
    dim_date.last_day_of_quarter                                            AS snapshot_last_day_of_quarter,
    dim_date.first_day_of_fiscal_quarter                                    AS snapshot_first_day_of_fiscal_quarter,
    dim_date.last_day_of_fiscal_quarter                                     AS snapshot_last_day_of_fiscal_quarter,
    dim_date.first_day_of_fiscal_year                                       AS snapshot_first_day_of_fiscal_year,
    dim_date.last_day_of_fiscal_year                                        AS snapshot_last_day_of_fiscal_year,
    dim_date.week_of_fiscal_year                                            AS snapshot_week_of_fiscal_year,
    dim_date.month_of_fiscal_year                                           AS snapshot_month_of_fiscal_year,
    dim_date.last_day_of_week                                               AS snapshot_last_day_of_week,
    dim_date.quarter_name                                                   AS snapshot_quarter_name,
    dim_date.fiscal_quarter_name_fy                                         AS snapshot_fiscal_quarter_name,
    dim_date.fiscal_quarter_name_fy                                         AS snapshot_fiscal_quarter_name_fy,
    dim_date.first_day_of_fiscal_quarter                                    AS snapshot_fiscal_quarter_date,
    dim_date.fiscal_quarter_number_absolute                                 AS snapshot_fiscal_quarter_number_absolute,
    dim_date.fiscal_month_name                                              AS snapshot_fiscal_month_name,
    dim_date.fiscal_month_name_fy                                           AS snapshot_fiscal_month_name_fy,
    dim_date.holiday_desc                                                   AS snapshot_holiday_desc,
    dim_date.is_holiday                                                     AS snapshot_is_holiday,
    dim_date.last_month_of_fiscal_quarter                                   AS snapshot_last_month_of_fiscal_quarter,
    dim_date.is_first_day_of_last_month_of_fiscal_quarter                   AS snapshot_is_first_day_of_last_month_of_fiscal_quarter,
    dim_date.last_month_of_fiscal_year                                      AS snapshot_last_month_of_fiscal_year,
    dim_date.is_first_day_of_last_month_of_fiscal_year                      AS snapshot_is_first_day_of_last_month_of_fiscal_year,
    dim_date.days_in_month_count                                            AS snapshot_days_in_month_count,
    dim_date.week_of_month_normalised                                       AS snapshot_week_of_month_normalised,
    dim_date.week_of_fiscal_quarter_normalised                              AS snapshot_week_of_fiscal_quarter_normalised,
    dim_date.is_first_day_of_fiscal_quarter_week                            AS snapshot_is_first_day_of_fiscal_quarter_week,
    dim_date.days_until_last_day_of_month                                   AS snapshot_days_until_last_day_of_month,
    dim_date.week_of_fiscal_quarter                                         AS snapshot_week_of_fiscal_quarter,
    dim_date.day_of_fiscal_quarter_normalised                               AS snapshot_day_of_fiscal_quarter_normalised,
    dim_date.day_of_fiscal_year_normalised                                  AS snapshot_day_of_fiscal_year_normalised,
    'targets_actuals'                                                       AS source,
    SUM(total_targets.pipeline_created_total_quarter_target)                AS pipeline_created_total_quarter_target,
    SUM(total_targets.net_arr_total_quarter_target)                         AS net_arr_total_quarter_target,
    CASE WHEN base.snapshot_date = dim_date.last_day_of_fiscal_quarter
        THEN SUM(quarterly_actuals.total_booked_net_arr)
      ELSE SUM(daily_actuals.booked_net_arr_in_snapshot_quarter)
    END                                                                     AS coverage_booked_net_arr,
    SUM(daily_actuals.open_1plus_net_arr_in_snapshot_quarter)               AS coverage_open_1plus_net_arr,
    SUM(daily_actuals.open_3plus_net_arr_in_snapshot_quarter)               AS coverage_open_3plus_net_arr,
    SUM(daily_actuals.open_4plus_net_arr_in_snapshot_quarter)               AS coverage_open_4plus_net_arr,
    SUM(quarterly_actuals.total_booked_net_arr)                             AS total_booked_net_arr
  FROM base
  LEFT JOIN total_targets
    ON base.snapshot_fiscal_quarter_name = total_targets.fiscal_quarter_name_fy
      AND base.dim_crm_current_account_set_hierarchy_sk = total_targets.dim_crm_user_hierarchy_sk
      AND base.sales_qualified_source_live = total_targets.sales_qualified_source_name
      AND base.sales_qualified_source_grouped_live = total_targets.sales_qualified_source_grouped
      AND base.order_type_live = total_targets.order_type_name
      AND base.order_type_grouped_live = total_targets.order_type_grouped
  LEFT JOIN daily_actuals
    ON base.snapshot_date = daily_actuals.snapshot_date
      AND base.dim_crm_current_account_set_hierarchy_sk = daily_actuals.dim_crm_current_account_set_hierarchy_sk
      AND base.sales_qualified_source_live = daily_actuals.sales_qualified_source_live
      AND base.sales_qualified_source_grouped_live = daily_actuals.sales_qualified_source_grouped_live
      AND base.order_type_live = daily_actuals.order_type_live
      AND base.order_type_grouped_live = daily_actuals.order_type_grouped_live
  LEFT JOIN quarterly_actuals
    ON base.snapshot_fiscal_quarter_name = quarterly_actuals.close_fiscal_quarter_name
      AND base.dim_crm_current_account_set_hierarchy_sk = quarterly_actuals.dim_crm_current_account_set_hierarchy_sk
      AND base.sales_qualified_source_live = quarterly_actuals.sales_qualified_source_name
      AND base.sales_qualified_source_grouped_live = quarterly_actuals.sales_qualified_source_grouped
      AND base.order_type_live = quarterly_actuals.order_type
      AND base.order_type_grouped_live = quarterly_actuals.order_type_grouped
  LEFT JOIN dim_date
    ON base.snapshot_date = dim_date.date_actual 
  LEFT JOIN hierarchy
    ON base.dim_crm_current_account_set_hierarchy_sk = hierarchy.dim_crm_user_hierarchy_sk
  GROUP BY ALL

)

SELECT *
FROM final