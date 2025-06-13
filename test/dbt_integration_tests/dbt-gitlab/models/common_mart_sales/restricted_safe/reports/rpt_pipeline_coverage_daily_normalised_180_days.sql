{{ simple_cte([
    ('targets', 'mart_sales_funnel_target_daily'),
    ('dim_date', 'dim_date'),
    ('hierarchy', 'dim_crm_user_hierarchy')
    ])
}},

opportunity_snapshot_base AS (

  SELECT *
  FROM {{ ref('mart_crm_opportunity_daily_snapshot') }}
  WHERE close_fiscal_quarter_date >= snapshot_fiscal_quarter_date

),

dim_date_for_close_quarters as (

    SELECT DISTINCT
      first_day_of_fiscal_quarter,
      fiscal_quarter_name_fy
    from dim_date

),

opportunity_live_base AS (

  SELECT *
  FROM {{ ref('mart_crm_opportunity') }} AS mart_crm_opportunity
  INNER JOIN dim_date AS close_date
    ON mart_crm_opportunity.close_date = close_date.date_actual

),

total_targets AS (

  SELECT
    targets.fiscal_quarter_name_fy,
    targets.order_type_name,
    targets.order_type_grouped,
    targets.sales_qualified_source_name,
    targets.sales_qualified_source_grouped,
    targets.dim_crm_user_hierarchy_sk,
    SUM(CASE WHEN kpi_name = 'Net ARR' THEN daily_allocated_target END)                                AS net_arr_total_quarter_target
  FROM targets
  {{ dbt_utils.group_by(n=6) }}

),

open_pipeline AS (

  SELECT
    opportunity_snapshot_base.dim_crm_current_account_set_hierarchy_sk,
    opportunity_snapshot_base.dim_crm_opportunity_id,
    opportunity_snapshot_base.snapshot_date,
    opportunity_snapshot_base.snapshot_fiscal_quarter_name,
    opportunity_snapshot_base.close_fiscal_quarter_name,
    opportunity_snapshot_base.close_fiscal_year,
    opportunity_snapshot_base.close_date,
    opportunity_snapshot_base.current_first_day_of_fiscal_year,
    opportunity_snapshot_base.snapshot_day_of_fiscal_quarter_normalised,
    DATEDIFF('day', close_date.last_day_of_fiscal_quarter, opportunity_snapshot_base.snapshot_date)+90 AS snapshot_date_normalised_180_days,

    opportunity_snapshot_base.sales_qualified_source_live,
    opportunity_snapshot_base.sales_qualified_source_grouped_live,
    opportunity_snapshot_base.order_type_live,
    opportunity_snapshot_base.order_type_grouped_live,
    
    opportunity_snapshot_base.open_1plus_net_arr,
    opportunity_snapshot_base.open_3plus_net_arr,
    opportunity_snapshot_base.open_4plus_net_arr,
    opportunity_snapshot_base.booked_net_arr

  FROM opportunity_snapshot_base
  LEFT JOIN dim_date AS close_date
    ON opportunity_snapshot_base.close_date= close_date.date_actual
  WHERE
    -- filtering only relevant opportunities for coverage reporting
    snapshot_date_normalised_180_days >=-92
    AND snapshot_date_normalised_180_days <= 90
    AND opportunity_snapshot_base.close_date > DATEADD('year', -1, opportunity_snapshot_base.current_first_day_of_fiscal_year)
    AND opportunity_snapshot_base.close_date <= DATEADD('year', +2, opportunity_snapshot_base.current_first_day_of_fiscal_year)

),

daily_actuals AS (

  SELECT
    open_pipeline.close_fiscal_quarter_name,
    open_pipeline.snapshot_date_normalised_180_days,
    open_pipeline.snapshot_fiscal_quarter_name,
    open_pipeline.snapshot_date,
    open_pipeline.sales_qualified_source_live,
    open_pipeline.sales_qualified_source_grouped_live,
    open_pipeline.order_type_live,
    open_pipeline.order_type_grouped_live,
    open_pipeline.dim_crm_current_account_set_hierarchy_sk,
    SUM(open_pipeline.booked_net_arr)                                                         AS booked_net_arr,
    SUM(open_pipeline.open_1plus_net_arr)                                                     AS open_1plus_net_arr,
    SUM(open_pipeline.open_3plus_net_arr)                                                     AS open_3plus_net_arr,
    SUM(open_pipeline.open_4plus_net_arr)                                                     AS open_4plus_net_arr
  FROM open_pipeline
  {{ dbt_utils.group_by(n=9) }}

),

quarterly_actuals AS (

  SELECT
    close_fiscal_quarter_name,
    close_fiscal_quarter_date,
    sales_qualified_source_name,
    sales_qualified_source_grouped,
    order_type,
    order_type_grouped,
    dim_crm_current_account_set_hierarchy_sk,
    SUM(booked_net_arr)                                                                     AS quarterly_total_booked_net_arr
  FROM opportunity_live_base
  {{ dbt_utils.group_by(n=7) }}

),

combined_data AS (

  SELECT
    dim_crm_current_account_set_hierarchy_sk,
    sales_qualified_source_live,
    sales_qualified_source_grouped_live,
    order_type_live,
    order_type_grouped_live,
    close_fiscal_quarter_name
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

spine AS (

  SELECT DISTINCT 
    snapshot_date,
    snapshot_date_normalised_180_days,
    close_fiscal_quarter_name
  FROM daily_actuals

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
    spine.snapshot_date,
    spine.snapshot_date_normalised_180_days,
    combined_data.close_fiscal_quarter_name
  FROM combined_data
  INNER JOIN spine
    ON combined_data.close_fiscal_quarter_name = spine.close_fiscal_quarter_name

),

final AS (

  SELECT

    {{ dbt_utils.generate_surrogate_key([
      'base.dim_crm_current_account_set_hierarchy_sk',
      'base.close_fiscal_quarter_name',
      'base.snapshot_date',
      'base.sales_qualified_source_live',
      'base.sales_qualified_source_grouped_live',
      'base.order_type_live',
      'base.order_type_grouped_live'])
    }}                                                                                                     AS rpt_pipeline_coverage_daily_pk,

    base.dim_crm_current_account_set_hierarchy_sk,
    base.close_fiscal_quarter_name,
    base.snapshot_date,
    base.sales_qualified_source_live,
    base.sales_qualified_source_grouped_live,
    base.order_type_live,
    base.order_type_grouped_live,
    base.snapshot_date_normalised_180_days,
    daily_actuals.snapshot_fiscal_quarter_name,

    hierarchy.crm_user_role_name                                                                           AS report_role_name,
    hierarchy.crm_user_role_level_1                                                                        AS report_role_level_1,
    hierarchy.crm_user_role_level_2                                                                        AS report_role_level_2,
    hierarchy.crm_user_role_level_3                                                                        AS report_role_level_3,

    dim_date.day_of_fiscal_quarter                                                                         AS snapshot_day_of_fiscal_quarter,
    close_date.first_day_of_fiscal_quarter                                                                 AS first_day_of_close_fiscal_quarter,
    today_date.current_first_day_of_fiscal_quarter,
    today_date.current_day_of_fiscal_quarter_normalised,                                           
    DATEDIFF('quarter', today_date.current_first_day_of_fiscal_quarter, first_day_of_close_fiscal_quarter) AS quarter_diff_current_and_close,
    
    -- This field facilitates the calculation of coverage for future quarters in Tableau
    LAST_VALUE(base.snapshot_date_normalised_180_days) 
      OVER (ORDER BY base.close_fiscal_quarter_name, base.snapshot_date ASC)                               AS future_quarters_current_date_normalised_180_days,

    -- The following fields facilitate selecting the most recent snapshot normalised day across multiple quarters
    FIRST_VALUE(base.snapshot_date_normalised_180_days) 
      OVER (ORDER BY base.snapshot_date DESC, base.snapshot_date_normalised_180_days DESC)                 AS most_recent_snapshot_date_normalised_180_days,
    MAX(base.snapshot_date)
      OVER ()                                                                                              AS most_recent_snapshot_date,

    SUM(total_targets.net_arr_total_quarter_target)                                                        AS net_arr_total_quarter_target,
    SUM(daily_actuals.booked_net_arr)                                                                      AS coverage_booked_net_arr,
    SUM(daily_actuals.open_1plus_net_arr)                                                                  AS coverage_open_1plus_net_arr,
    SUM(daily_actuals.open_3plus_net_arr)                                                                  AS coverage_open_3plus_net_arr,
    SUM(daily_actuals.open_4plus_net_arr)                                                                  AS coverage_open_4plus_net_arr,
    SUM(quarterly_actuals.quarterly_total_booked_net_arr)                                                  AS quarterly_total_booked_net_arr,

    /*
    Pipeline Coverage Calculation:
    Pipeline Coverage = (Open Pipeline) / ((Target or Actual) - Net ARR QTD)

    The denominator (Target or Actual) is determined by comparing close_fiscal_quarter to current_date:
    - If close_fiscal_quarter is in the past quarters: Uses "Actual" (quarterly_total_booked_net_arr)
    - If close_fiscal_quarter is in current or future quarters: Uses "Target" (net_arr_total_quarter_target)

    Note: The pipeline coverage calculation is performed in the Tableau layer since it involves division.
    */
    CASE 
      WHEN first_day_of_close_fiscal_quarter < today_date.current_first_day_of_fiscal_quarter
        THEN SUM(quarterly_actuals.quarterly_total_booked_net_arr)
      ELSE SUM(total_targets.net_arr_total_quarter_target)
    END                                                                     AS target_or_actual_for_coverage_denominator 

  FROM base
  LEFT JOIN total_targets
    ON base.close_fiscal_quarter_name = total_targets.fiscal_quarter_name_fy
      AND base.dim_crm_current_account_set_hierarchy_sk = total_targets.dim_crm_user_hierarchy_sk
      AND base.sales_qualified_source_live = total_targets.sales_qualified_source_name
      AND base.sales_qualified_source_grouped_live = total_targets.sales_qualified_source_grouped
      AND base.order_type_live = total_targets.order_type_name
      AND base.order_type_grouped_live = total_targets.order_type_grouped

  LEFT JOIN daily_actuals
    ON base.snapshot_date = daily_actuals.snapshot_date
      AND base.close_fiscal_quarter_name = daily_actuals.close_fiscal_quarter_name
      AND base.dim_crm_current_account_set_hierarchy_sk = daily_actuals.dim_crm_current_account_set_hierarchy_sk
      AND base.sales_qualified_source_live = daily_actuals.sales_qualified_source_live
      AND base.sales_qualified_source_grouped_live = daily_actuals.sales_qualified_source_grouped_live
      AND base.order_type_live = daily_actuals.order_type_live
      AND base.order_type_grouped_live = daily_actuals.order_type_grouped_live

  LEFT JOIN quarterly_actuals
    ON base.close_fiscal_quarter_name = quarterly_actuals.close_fiscal_quarter_name
      AND base.dim_crm_current_account_set_hierarchy_sk = quarterly_actuals.dim_crm_current_account_set_hierarchy_sk
      AND base.sales_qualified_source_live = quarterly_actuals.sales_qualified_source_name
      AND base.sales_qualified_source_grouped_live = quarterly_actuals.sales_qualified_source_grouped
      AND base.order_type_live = quarterly_actuals.order_type
      AND base.order_type_grouped_live = quarterly_actuals.order_type_grouped

  LEFT JOIN dim_date
    ON base.snapshot_date = dim_date.date_actual 
  LEFT JOIN dim_date_for_close_quarters AS close_date
    ON base.close_fiscal_quarter_name = close_date.fiscal_quarter_name_fy
  INNER JOIN dim_date AS today_date
    ON today_date.date_actual = CURRENT_DATE()  
  LEFT JOIN hierarchy
    ON base.dim_crm_current_account_set_hierarchy_sk = hierarchy.dim_crm_user_hierarchy_sk  
  {{ dbt_utils.group_by(n=18) }}

)

SELECT * FROM final