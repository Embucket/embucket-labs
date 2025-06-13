WITH live_actuals AS (
  
  SELECT 
    dim_crm_current_account_set_hierarchy_sk,
    report_geo,
    report_business_unit,
    report_segment,
    report_region,
    report_area,
    report_role_name,
    report_role_level_1,
    report_role_level_2,
    report_role_level_3,
    report_role_level_4,
    report_role_level_5,
    close_fiscal_quarter_date,
    arr_created_fiscal_quarter_date,
    sales_qualified_source_name,
    sales_qualified_source_grouped,
    order_type,
    order_type_grouped,
    deal_path_name,
    subscription_type,
    stage_name,
    booked_net_arr,
    booked_deal_count,
    closed_won_opps,
    created_arr,
    created_deals
  FROM {{ref('mart_crm_opportunity')}}

),

snapshot_date AS (

  SELECT DISTINCT
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
    dim_date.fiscal_quarter_name_fy,
    dim_date.fiscal_quarter,
    dim_date.fiscal_year,
    dim_date.last_day_of_fiscal_quarter,
    dim_date.first_day_of_fiscal_year,
    dim_date.last_day_of_fiscal_year,
    dim_date.first_day_of_fiscal_quarter,
    dim_date.fiscal_quarter_number_absolute,
    dim_date.last_month_of_fiscal_quarter,
    dim_date.last_month_of_fiscal_year,
    dim_date.days_in_fiscal_quarter_count
  FROM {{ref('dim_date')}}
  WHERE current_first_day_of_fiscal_quarter > first_day_of_fiscal_quarter
    AND first_day_of_fiscal_quarter >= DATEADD(QUARTER, -9, CURRENT_DATE())

),

booked_arr AS (

  SELECT
    live_actuals.dim_crm_current_account_set_hierarchy_sk,
    live_actuals.report_geo,
    live_actuals.report_business_unit,
    live_actuals.report_segment,
    live_actuals.report_region,
    live_actuals.report_area,
    live_actuals.report_role_name,
    live_actuals.report_role_level_1,
    live_actuals.report_role_level_2,
    live_actuals.report_role_level_3,
    live_actuals.report_role_level_4,
    live_actuals.report_role_level_5,
    live_actuals.close_fiscal_quarter_date,
    live_actuals.sales_qualified_source_name,
    live_actuals.sales_qualified_source_grouped,
    live_actuals.order_type,
    live_actuals.order_type_grouped,
    live_actuals.deal_path_name,
    live_actuals.subscription_type,
    live_actuals.stage_name,
    SUM(live_actuals.booked_net_arr) AS total_booked_arr,
    SUM(live_actuals.booked_deal_count) AS total_booked_deal_count,
    SUM(live_actuals.closed_won_opps) AS total_closed_won_opps,
    SUM(CASE WHEN live_actuals.booked_net_arr > 0 THEN live_actuals.booked_net_arr ELSE NULL END) AS total_positive_booked_net_arr,
    SUM(CASE WHEN live_actuals.booked_net_arr > 0 THEN 1 ELSE NULL END) AS total_positive_booked_deal_count
  FROM live_actuals
  {{ dbt_utils.group_by(n=20) }}

),

created_arr AS (

  SELECT
    live_actuals.dim_crm_current_account_set_hierarchy_sk,
    live_actuals.report_geo,
    live_actuals.report_business_unit,
    live_actuals.report_segment,
    live_actuals.report_region,
    live_actuals.report_area,
    live_actuals.report_role_name,
    live_actuals.report_role_level_1,
    live_actuals.report_role_level_2,
    live_actuals.report_role_level_3,
    live_actuals.report_role_level_4,
    live_actuals.report_role_level_5,
    live_actuals.arr_created_fiscal_quarter_date,
    live_actuals.sales_qualified_source_name,
    live_actuals.sales_qualified_source_grouped,
    live_actuals.order_type,
    live_actuals.order_type_grouped,
    live_actuals.deal_path_name,
    live_actuals.subscription_type,
    live_actuals.stage_name,
    SUM(live_actuals.created_arr) AS total_created_arr,
    SUM(live_actuals.created_deals) AS total_created_deal_count
  FROM live_actuals
  {{ dbt_utils.group_by(n=20) }}

),

base AS (

  SELECT
    booked_arr.dim_crm_current_account_set_hierarchy_sk,
    booked_arr.report_geo,
    booked_arr.report_business_unit,
    booked_arr.report_segment,
    booked_arr.report_region,
    booked_arr.report_area,
    booked_arr.report_role_name,
    booked_arr.report_role_level_1,
    booked_arr.report_role_level_2,
    booked_arr.report_role_level_3,
    booked_arr.report_role_level_4,
    booked_arr.report_role_level_5,
    booked_arr.close_fiscal_quarter_date AS snapshot_fiscal_quarter_date,
    booked_arr.sales_qualified_source_name,
    booked_arr.sales_qualified_source_grouped,
    booked_arr.order_type,
    booked_arr.order_type_grouped,
    booked_arr.deal_path_name,
    booked_arr.subscription_type,
    booked_arr.stage_name
  FROM booked_arr

  UNION 

  SELECT
    created_arr.dim_crm_current_account_set_hierarchy_sk,
    created_arr.report_geo,
    created_arr.report_business_unit,
    created_arr.report_segment,
    created_arr.report_region,
    created_arr.report_area,
    created_arr.report_role_name,
    created_arr.report_role_level_1,
    created_arr.report_role_level_2,
    created_arr.report_role_level_3,
    created_arr.report_role_level_4,
    created_arr.report_role_level_5,
    created_arr.arr_created_fiscal_quarter_date,
    created_arr.sales_qualified_source_name,
    created_arr.sales_qualified_source_grouped,
    created_arr.order_type,
    created_arr.order_type_grouped,
    created_arr.deal_path_name,
    created_arr.subscription_type,
    created_arr.stage_name
  FROM created_arr

),

final AS (

  SELECT
    base.dim_crm_current_account_set_hierarchy_sk,
    base.report_segment,
    base.report_business_unit,
    base.report_geo,
    base.report_region,
    base.report_area,
    base.report_role_name,
    base.report_role_level_1,
    base.report_role_level_2,
    base.report_role_level_3,
    base.report_role_level_4,
    base.report_role_level_5,
    base.sales_qualified_source_name                                               AS sales_qualified_source_live,
    base.sales_qualified_source_grouped                                            AS sales_qualified_source_grouped_live,
    base.order_type                                                                AS order_type_live,
    base.order_type_grouped                                                        AS order_type_grouped_live,
    base.deal_path_name                                                            AS deal_path_live,
    base.subscription_type,
    base.stage_name, 
    snapshot_date.current_day_name,
    snapshot_date.current_date_actual,
    snapshot_date.current_fiscal_year,
    snapshot_date.current_first_day_of_fiscal_year,
    snapshot_date.current_fiscal_quarter_name_fy,
    snapshot_date.current_first_day_of_month,
    snapshot_date.current_first_day_of_fiscal_quarter,
    snapshot_date.current_day_of_month,
    snapshot_date.current_day_of_fiscal_quarter,
    snapshot_date.current_day_of_fiscal_year,
    snapshot_date.current_first_day_of_week,
    snapshot_date.current_week_of_fiscal_quarter_normalised,
    snapshot_date.current_week_of_fiscal_quarter,
    snapshot_date.fiscal_quarter_name_fy                                           AS snapshot_fiscal_quarter_name,
    snapshot_date.fiscal_quarter_name_fy                                           AS snapshot_fiscal_quarter_name_fy,
    snapshot_date.fiscal_quarter                                                   AS snapshot_fiscal_quarter,
    snapshot_date.fiscal_year                                                      AS snapshot_fiscal_year,
    snapshot_date.first_day_of_fiscal_quarter                                      AS snapshot_first_day_of_fiscal_quarter,
    snapshot_date.last_day_of_fiscal_quarter                                       AS snapshot_last_day_of_fiscal_quarter,
    snapshot_date.first_day_of_fiscal_year                                         AS snapshot_first_day_of_fiscal_year,
    snapshot_date.last_day_of_fiscal_year                                          AS snapshot_last_day_of_fiscal_year,
    snapshot_date.first_day_of_fiscal_quarter                                      AS snapshot_fiscal_quarter_date,
    snapshot_date.fiscal_quarter_number_absolute                                   AS snapshot_fiscal_quarter_number_absolute,
    snapshot_date.last_month_of_fiscal_quarter                                     AS snapshot_last_month_of_fiscal_quarter,
    snapshot_date.last_month_of_fiscal_year                                        AS snapshot_last_month_of_fiscal_year,
    snapshot_date.days_in_fiscal_quarter_count                                     AS snapshot_days_in_fiscal_quarter_count,
    '94'                                                                           AS snapshot_day_of_fiscal_quarter, 
    'final_bookings'                                                               AS source,
    booked_arr.total_booked_arr                                                    AS booked_net_arr_in_snapshot_quarter,
    created_arr.total_created_arr                                                  AS created_arr_in_snapshot_quarter,
    created_arr.total_created_deal_count                                           AS created_deals_in_snapshot_quarter,
    booked_arr.total_booked_deal_count                                             AS booked_deal_count_in_snapshot_quarter,  
    booked_arr.total_closed_won_opps                                               AS closed_won_opps_in_snapshot_quarter,  
    booked_arr.total_positive_booked_net_arr                                       AS positive_booked_net_arr_in_snapshot_quarter,  
    booked_arr.total_positive_booked_deal_count                                    AS positive_booked_deal_count_in_snapshot_quarter  
  FROM base
  INNER JOIN snapshot_date
    ON base.snapshot_fiscal_quarter_date = snapshot_date.first_day_of_fiscal_quarter
  LEFT JOIN created_arr
    ON base.snapshot_fiscal_quarter_date = created_arr.arr_created_fiscal_quarter_date
      AND base.sales_qualified_source_name = created_arr.sales_qualified_source_name
      AND base.sales_qualified_source_grouped = created_arr.sales_qualified_source_grouped
      AND base.order_type = created_arr.order_type
      AND base.order_type_grouped = created_arr.order_type_grouped
      AND base.deal_path_name = created_arr.deal_path_name
      AND base.subscription_type = created_arr.subscription_type
      AND base.stage_name = created_arr.stage_name
      AND base.dim_crm_current_account_set_hierarchy_sk = created_arr.dim_crm_current_account_set_hierarchy_sk
  LEFT JOIN booked_arr
    ON base.snapshot_fiscal_quarter_date = booked_arr.close_fiscal_quarter_date
      AND base.sales_qualified_source_name = booked_arr.sales_qualified_source_name
      AND base.sales_qualified_source_grouped = booked_arr.sales_qualified_source_grouped
      AND base.order_type = booked_arr.order_type
      AND base.order_type_grouped = booked_arr.order_type_grouped
      AND base.deal_path_name = booked_arr.deal_path_name
      AND base.subscription_type = booked_arr.subscription_type
      AND base.stage_name = booked_arr.stage_name
      AND base.dim_crm_current_account_set_hierarchy_sk = booked_arr.dim_crm_current_account_set_hierarchy_sk

)

SELECT *
FROM final
