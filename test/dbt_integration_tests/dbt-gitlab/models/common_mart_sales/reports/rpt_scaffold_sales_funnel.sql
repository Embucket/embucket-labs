{{ config(
    tags=["mnpi_exception", "six_hourly"]
) }}

{{ simple_cte([
    ('fct_sales_funnel_target_daily', 'fct_sales_funnel_target_daily'),
    ('fct_sales_funnel_actual', 'fct_sales_funnel_actual'),
    ('dim_sales_funnel_kpi', 'dim_sales_funnel_kpi')
]) }}

, final AS (

    SELECT
      fct_sales_funnel_actual.actual_date_id                                              AS date_id,
      dim_sales_funnel_kpi.dim_sales_funnel_kpi_sk,
      fct_sales_funnel_actual.dim_hierarchy_sk,
      fct_sales_funnel_actual.dim_order_type_id,
      fct_sales_funnel_actual.dim_sales_qualified_source_id
    FROM fct_sales_funnel_actual
    LEFT JOIN dim_sales_funnel_kpi
      ON dim_sales_funnel_kpi.sales_funnel_kpi_name = fct_sales_funnel_actual.sales_funnel_kpi_name

    UNION

    SELECT 
      fct_sales_funnel_target_daily.target_date_id,
      dim_sales_funnel_kpi.dim_sales_funnel_kpi_sk,
      fct_sales_funnel_target_daily.dim_crm_user_hierarchy_sk,
      fct_sales_funnel_target_daily.dim_order_type_id,
      fct_sales_funnel_target_daily.dim_sales_qualified_source_id
    FROM fct_sales_funnel_target_daily
    LEFT JOIN dim_sales_funnel_kpi
      ON dim_sales_funnel_kpi.sales_funnel_kpi_name = fct_sales_funnel_target_daily.kpi_name

)

SELECT *
FROM final
