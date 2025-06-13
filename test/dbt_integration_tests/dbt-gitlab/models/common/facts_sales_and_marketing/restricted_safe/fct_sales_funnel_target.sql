{{ simple_cte([
      ('prep_crm_user_hierarchy', 'prep_crm_user_hierarchy'),
      ('sales_qualified_source', 'prep_sales_qualified_source'),
      ('order_type', 'prep_order_type'),
      ('prep_sales_funnel_target', 'prep_sales_funnel_target'),
      ('prep_sales_funnel_kpi', 'prep_sales_funnel_kpi'),
      ('prep_date', 'prep_date')
])}}

, final_targets AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['prep_sales_funnel_target.dim_crm_user_hierarchy_sk',
                                 'prep_sales_funnel_target.fiscal_year', 
                                 'prep_sales_funnel_target.kpi_name', 
                                 'prep_sales_funnel_target.first_day_of_month', 
                                 'prep_sales_funnel_target.sales_qualified_source',
                                 'prep_sales_funnel_target.order_type',
                                 'prep_sales_funnel_target.user_geo',
                                 'prep_sales_funnel_target.user_segment'
                                 ]) }}                                                AS sales_funnel_target_id,
     prep_sales_funnel_target.kpi_name,
     prep_sales_funnel_kpi.dim_sales_funnel_kpi_sk,
     prep_date.date_id                                                                AS target_month_id,
     prep_sales_funnel_target.first_day_of_month,
     prep_sales_funnel_target.fiscal_year,
     prep_sales_funnel_target.sales_qualified_source                                  AS sales_qualified_source,
     {{ get_keyed_nulls('sales_qualified_source.dim_sales_qualified_source_id') }}    AS dim_sales_qualified_source_id,
     prep_sales_funnel_target.order_type,
     {{ get_keyed_nulls('order_type.dim_order_type_id') }}                            AS dim_order_type_id,
     prep_sales_funnel_target.area                                                    AS crm_user_sales_segment_geo_region_area,
     prep_crm_user_hierarchy.dim_crm_user_hierarchy_id                                AS dim_crm_user_hierarchy_live_id,
     prep_crm_user_hierarchy.dim_crm_user_business_unit_id,
     prep_crm_user_hierarchy.dim_crm_user_sales_segment_id,
     prep_crm_user_hierarchy.dim_crm_user_geo_id,
     prep_crm_user_hierarchy.dim_crm_user_region_id,
     prep_crm_user_hierarchy.dim_crm_user_area_id,
     prep_crm_user_hierarchy.dim_crm_user_hierarchy_id                                AS dim_crm_user_hierarchy_stamped_id,
     {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_hierarchy_sk') }}       AS dim_crm_user_hierarchy_sk,
     prep_sales_funnel_target.dim_crm_user_hierarchy_sk                               AS target_dim_crm_user_hierarchy_sk,
     prep_crm_user_hierarchy.dim_crm_user_business_unit_id                            AS dim_crm_opp_owner_business_unit_stamped_id,
     prep_crm_user_hierarchy.dim_crm_user_sales_segment_id                            AS dim_crm_opp_owner_sales_segment_stamped_id,
     prep_crm_user_hierarchy.dim_crm_user_geo_id                                      AS dim_crm_opp_owner_geo_stamped_id,
     prep_crm_user_hierarchy.dim_crm_user_region_id                                   AS dim_crm_opp_owner_region_stamped_id,
     prep_crm_user_hierarchy.dim_crm_user_area_id                                     AS dim_crm_opp_owner_area_stamped_id,
     prep_crm_user_hierarchy.dim_crm_user_role_name_id                                AS dim_crm_opp_owner_role_name_id,
     prep_crm_user_hierarchy.dim_crm_user_role_level_1_id                             AS dim_crm_opp_owner_role_level_1_id,
     prep_crm_user_hierarchy.dim_crm_user_role_level_2_id                             AS dim_crm_opp_owner_role_level_2_id,
     prep_crm_user_hierarchy.dim_crm_user_role_level_3_id                             AS dim_crm_opp_owner_role_level_3_id,
     prep_crm_user_hierarchy.dim_crm_user_role_level_4_id                             AS dim_crm_opp_owner_role_level_4_id,
     prep_crm_user_hierarchy.dim_crm_user_role_level_5_id                             AS dim_crm_opp_owner_role_level_5_id,
     SUM(prep_sales_funnel_target.allocated_target)                                   AS allocated_target
    FROM prep_sales_funnel_target
    LEFT JOIN sales_qualified_source
      ON {{ sales_funnel_text_slugify("prep_sales_funnel_target.sales_qualified_source") }} = {{ sales_funnel_text_slugify("sales_qualified_source.sales_qualified_source_name") }}
    LEFT JOIN order_type
      ON {{ sales_funnel_text_slugify("prep_sales_funnel_target.order_type") }} = {{ sales_funnel_text_slugify("order_type.order_type_name") }}
    LEFT JOIN prep_crm_user_hierarchy
      ON prep_sales_funnel_target.dim_crm_user_hierarchy_sk = prep_crm_user_hierarchy.dim_crm_user_hierarchy_sk
        AND prep_sales_funnel_target.fiscal_year = prep_crm_user_hierarchy.fiscal_year
    LEFT JOIN prep_sales_funnel_kpi
      ON {{ sales_funnel_text_slugify("prep_sales_funnel_kpi.sales_funnel_kpi_name") }} = {{ sales_funnel_text_slugify("prep_sales_funnel_target.kpi_name") }}
    LEFT JOIN prep_date
      ON prep_date.date_actual = prep_sales_funnel_target.first_day_of_month
    {{ dbt_utils.group_by(n=31)}}

)

{{ dbt_audit(
    cte_ref="final_targets",
    created_by="@michellecooper",
    updated_by="@chrissharp",
    created_date="2023-02-07",
    updated_date="2024-04-17"
) }}
