{{ simple_cte([
      ('dim_crm_user_hierarchy', 'dim_crm_user_hierarchy'),
      ('dim_sales_qualified_source', 'dim_sales_qualified_source'),
      ('dim_order_type', 'dim_order_type'),
      ('fct_sales_funnel_target', 'fct_sales_funnel_target'),
      ('dim_sales_funnel_kpi', 'dim_sales_funnel_kpi')
])}}

, final AS (

    SELECT
      fct_sales_funnel_target.sales_funnel_target_id,
      fct_sales_funnel_target.first_day_of_month      AS target_month,
      dim_sales_funnel_kpi.sales_funnel_kpi_name      AS kpi_name,
      dim_crm_user_hierarchy.crm_user_sales_segment,
      dim_crm_user_hierarchy.crm_user_sales_segment_grouped,
      dim_crm_user_hierarchy.crm_user_business_unit,
      dim_crm_user_hierarchy.crm_user_geo,
      dim_crm_user_hierarchy.crm_user_region,
      dim_crm_user_hierarchy.crm_user_area,
      dim_crm_user_hierarchy.crm_user_sales_segment_region_grouped,
      dim_order_type.order_type_name,
      dim_order_type.order_type_grouped,
      dim_sales_qualified_source.sales_qualified_source_name,
      dim_sales_qualified_source.sales_qualified_source_grouped,
      fct_sales_funnel_target.allocated_target
    FROM fct_sales_funnel_target
    LEFT JOIN dim_sales_qualified_source
      ON fct_sales_funnel_target.dim_sales_qualified_source_id = dim_sales_qualified_source.dim_sales_qualified_source_id
    LEFT JOIN dim_order_type
      ON fct_sales_funnel_target.dim_order_type_id = dim_order_type.dim_order_type_id
    LEFT JOIN dim_crm_user_hierarchy
      ON fct_sales_funnel_target.dim_crm_user_hierarchy_sk = dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk
    LEFT JOIN dim_sales_funnel_kpi
      ON fct_sales_funnel_target.dim_sales_funnel_kpi_sk = dim_sales_funnel_kpi.dim_sales_funnel_kpi_sk

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@kjpeguero",
    created_date="2021-01-08",
    updated_date="2023-10-27",
  ) }}
