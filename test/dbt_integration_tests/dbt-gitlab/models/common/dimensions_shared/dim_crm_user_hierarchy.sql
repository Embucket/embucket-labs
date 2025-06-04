{{ config(
    tags=["mnpi_exception"]
) }}


WITH source AS (


  SELECT
    dim_crm_user_hierarchy_id,
    dim_crm_user_hierarchy_sk,
    fiscal_year,
    crm_user_business_unit,
    dim_crm_user_business_unit_id,
    crm_user_sales_segment,
    dim_crm_user_sales_segment_id,
    crm_user_geo,
    dim_crm_user_geo_id,
    crm_user_region,
    dim_crm_user_region_id,
    crm_user_area,
    dim_crm_user_area_id,
    crm_user_sales_segment_grouped,
    crm_user_sales_segment_region_grouped,
    crm_user_role_name,
    dim_crm_user_role_name_id,
    crm_user_role_level_1,
    dim_crm_user_role_level_1_id,
    crm_user_role_level_2,
    dim_crm_user_role_level_2_id,
    crm_user_role_level_3,
    dim_crm_user_role_level_3_id,
    crm_user_role_level_4,
    dim_crm_user_role_level_4_id,
    crm_user_role_level_5,
    dim_crm_user_role_level_5_id,
    pipe_council_grouping,
    is_current_crm_user_hierarchy,
    crm_user_geo_pubsec_segment
  FROM {{ ref('prep_crm_user_hierarchy') }}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@mcooperDD",
    updated_by="@chrissharp",
    created_date="2021-01-05",
    updated_date="2024-04-23"
) }}
