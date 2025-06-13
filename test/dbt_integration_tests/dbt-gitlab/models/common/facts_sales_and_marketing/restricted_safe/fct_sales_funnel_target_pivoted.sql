{{ simple_cte([
    ('dim_date','dim_date'),
    ('targets', 'fct_sales_funnel_target')
]) }}, 

targets_dates AS (

  SELECT DISTINCT
    targets.first_day_of_month AS target_month_date,
    targets.target_month_id AS target_month_date_id,
    targets.first_day_of_month AS report_target_date,
    dim_date.fiscal_quarter_name_fy,
    targets.fiscal_year,
    targets.dim_crm_user_hierarchy_sk,
    targets.dim_sales_qualified_source_id,
    targets.dim_order_type_id,
    targets.kpi_name,
    targets.allocated_target
  FROM targets
  LEFT JOIN dim_date
    ON targets.target_month_id = dim_date.date_id

),

pivoted_targets AS (

  SELECT
    target_month_date,
    target_month_date_id,
    fiscal_quarter_name_fy,
    fiscal_year,
    dim_crm_user_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    "'Deals'"                       AS deals_created_monthly_allocated_target,
    "'New Logos'"                   AS new_logos_created_monthly_allocated_target,
    "'Stage 1 Opportunities'"       AS saos_created_monthly_allocated_target,
    "'Net ARR'"                     AS net_arr_monthly_allocated_target,
    "'ATR'"                         AS atr_created_monthly_allocated_target,
    "'Partner Net ARR'"             AS partner_net_arr_monthly_allocated_target,
    "'PS Value'"                    AS ps_value_monthly_allocated_target,
    "'PS Value Pipeline Created'"   AS ps_value_pipeline_created_monthly_allocated_target,
    "'Net ARR Pipeline Created'"    AS net_arr_pipeline_created_monthly_allocated_target,
    "'Churn/Contraction Amount'"    AS churn_contraction_amount_monthly_allocated_target
  FROM (
    SELECT *
    FROM targets_dates
    PIVOT (
      SUM(allocated_target)
      FOR kpi_name IN ('Net ARR', 'Deals', 'New Logos', 'Stage 1 Opportunities', 'Net ARR Pipeline Created', 'Partner Net ARR', 'PS Value Pipeline Created', 'PS Value', 'Churn/Contraction Amount', 'ATR')
    ) AS pvt
  )

),

pivoted_targets_quarter AS (

  SELECT
    fiscal_quarter_name_fy,
    dim_crm_user_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    -- need a date to join on for the fiscal quarter
    MIN(target_month_date_id)                                AS target_quarter_date_id,
    SUM(deals_created_monthly_allocated_target)              AS deals_quarterly_allocated_target,
    SUM(new_logos_created_monthly_allocated_target)          AS new_logos_quarterly_allocated_target,
    SUM(saos_created_monthly_allocated_target)               AS saos_quarterly_allocated_target,
    SUM(net_arr_monthly_allocated_target)                    AS net_arr_quarterly_allocated_target,
    SUM(atr_created_monthly_allocated_target)                AS atr_quarterly_allocated_target,
    SUM(partner_net_arr_monthly_allocated_target)            AS partner_net_arr_quarterly_allocated_target,
    SUM(ps_value_monthly_allocated_target)                   AS ps_value_quarterly_allocated_target,
    SUM(ps_value_pipeline_created_monthly_allocated_target)  AS ps_value_pipeline_created_quarterly_allocated_target,
    SUM(net_arr_pipeline_created_monthly_allocated_target)   AS net_arr_pipeline_created_quarterly_allocated_target,
    SUM(churn_contraction_amount_monthly_allocated_target)   AS churn_contraction_amount_quarterly_allocated_target
  FROM pivoted_targets
  GROUP BY 1, 2, 3, 4

),

final AS (

SELECT
    pivoted_targets.*,
    deals_quarterly_allocated_target,
    new_logos_quarterly_allocated_target,
    saos_quarterly_allocated_target,
    net_arr_quarterly_allocated_target,
    atr_quarterly_allocated_target,
    partner_net_arr_quarterly_allocated_target,
    ps_value_quarterly_allocated_target,
    ps_value_pipeline_created_quarterly_allocated_target,
    net_arr_pipeline_created_quarterly_allocated_target,
    churn_contraction_amount_quarterly_allocated_target
FROM pivoted_targets
LEFT JOIN pivoted_targets_quarter
ON pivoted_targets.DIM_ORDER_TYPE_ID = pivoted_targets_quarter.DIM_ORDER_TYPE_ID
AND pivoted_targets.dim_crm_user_hierarchy_sk = pivoted_targets_quarter.dim_crm_user_hierarchy_sk
AND pivoted_targets.dim_sales_qualified_source_id = pivoted_targets_quarter.dim_sales_qualified_source_id
AND pivoted_targets.target_month_date_id = pivoted_targets_quarter.target_quarter_date_id

)

SELECT * 
FROM final
