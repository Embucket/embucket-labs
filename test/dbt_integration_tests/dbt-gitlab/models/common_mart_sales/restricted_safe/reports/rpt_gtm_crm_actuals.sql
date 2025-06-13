{{ config(
    tags=["six_hourly"]
) }}

{{ simple_cte([
    ('fct_crm_opportunity','fct_crm_opportunity'),
    ('dim_date','dim_date'),
    ('mart_crm_opportunity', 'mart_crm_opportunity')
]) }},

created_actuals AS (
  SELECT
    dim_crm_opportunity_id,
    dim_order_type_id,
    dim_sales_qualified_source_id,
    dim_crm_current_account_set_hierarchy_sk,
    arr_created_date AS actual_date,
    CASE WHEN is_net_arr_pipeline_created = TRUE
        AND arr_created_date IS NOT NULL
        THEN net_arr
    END              AS net_arr_pipeline_created,
    NULL             AS net_arr_closed,
    NULL             AS new_logo_count_closed,
    NULL             AS deal_ids_count,
    NULL             AS sao_net_arr,
    NULL             AS sao_opportunity_id_count
  FROM fct_crm_opportunity
  WHERE is_net_arr_pipeline_created = TRUE
    AND arr_created_date IS NOT NULL
),

closed_actuals AS (
  SELECT
    dim_crm_opportunity_id,
    dim_order_type_id,
    dim_sales_qualified_source_id,
    dim_crm_current_account_set_hierarchy_sk,
    close_date AS actual_date,
    NULL       AS net_arr_pipeline_created,
    CASE
      WHEN fpa_master_bookings_flag = TRUE
        THEN booked_net_arr
    END        AS net_arr_closed,
    CASE
      WHEN is_new_logo_first_order = TRUE
        THEN new_logo_count
    END        AS new_logo_count_closed,
    CASE
      WHEN new_logo_count >= 0
        THEN 1
      WHEN new_logo_count = -1 THEN -1
    END        AS deal_ids_count,
    NULL       AS sao_net_arr,
    NULL       AS sao_opportunity_id_count
  FROM fct_crm_opportunity
  WHERE fpa_master_bookings_flag = TRUE
),

sao_actuals AS (
  SELECT
    dim_crm_opportunity_id,
    dim_order_type_id,
    dim_sales_qualified_source_id,
    dim_crm_current_account_set_hierarchy_sk,
    sales_accepted_date AS actual_date,
    NULL                AS net_arr_pipeline_created,
    NULL                AS net_arr_closed,
    NULL                AS new_logo_count_closed,
    NULL                AS deal_ids_count,
    CASE WHEN fpa_master_bookings_flag = TRUE
        THEN net_arr
    END                 AS sao_net_arr,
    CASE WHEN is_sao = TRUE
        THEN 1
    END                 AS sao_opportunity_id_count
  FROM fct_crm_opportunity
),

--union
unioned_actuals AS (
  SELECT *
  FROM created_actuals

  UNION

  SELECT *
  FROM closed_actuals

  UNION

  SELECT *
  FROM sao_actuals

),

main_actuals AS (
  SELECT
    unioned_actuals.dim_crm_opportunity_id,
    unioned_actuals.dim_order_type_id,
    unioned_actuals.dim_sales_qualified_source_id,
    unioned_actuals.dim_crm_current_account_set_hierarchy_sk,
    unioned_actuals.actual_date,
    unioned_actuals.net_arr_pipeline_created,
    unioned_actuals.net_arr_closed,
    unioned_actuals.new_logo_count_closed,
    unioned_actuals.deal_ids_count,
    unioned_actuals.sao_net_arr,
    unioned_actuals.sao_opportunity_id_count AS sao_count,
    dim_date.date_id                         AS actual_date_id,
    mart_crm_opportunity.product_category,
    mart_crm_opportunity.deal_size,
    dim_date.fiscal_quarter_name_fy,
    dim_date.fiscal_year
  FROM unioned_actuals
  LEFT JOIN dim_date
    ON unioned_actuals.actual_date = dim_date.date_actual
  LEFT JOIN mart_crm_opportunity
    ON unioned_actuals.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
  WHERE fiscal_year > 2019
  ORDER BY 1 DESC
)

SELECT
  *
FROM main_actuals
