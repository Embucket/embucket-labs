{{ config(
    tags=["mnpi_exception"] 
) }}

WITH account_dimensions AS (

    SELECT *
    FROM {{ ref('map_crm_account') }}

), bizible_touchpoints AS (

    SELECT *
    FROM {{ ref('prep_crm_touchpoint') }}

), crm_person AS (

    SELECT 
      bizible_person_id,
      dim_crm_person_id,
      dim_crm_user_id
    FROM {{ ref('prep_crm_person') }}
    -- occasionally we see duplicate bizible_person_id's in prep_crm_person so we should qualify on that field
    QUALIFY ROW_NUMBER() OVER (PARTITION BY bizible_person_id ORDER BY created_date DESC) = 1

), final_touchpoint AS (

    SELECT
      touchpoint_id                             AS dim_crm_touchpoint_id,
      bizible_touchpoints.bizible_person_id,

      -- shared dimension keys
      crm_person.dim_crm_person_id,
      crm_person.dim_crm_user_id,
      campaign_id                                       AS dim_campaign_id,
      account_dimensions.dim_crm_account_id,
      account_dimensions.dim_parent_crm_account_id,
      account_dimensions.dim_parent_sales_segment_id,
      account_dimensions.dim_parent_sales_territory_id,
      account_dimensions.dim_parent_industry_id,
      account_dimensions.dim_account_sales_segment_id,
      account_dimensions.dim_account_sales_territory_id,
      account_dimensions.dim_account_industry_id,
      account_dimensions.dim_account_location_country_id,
      account_dimensions.dim_account_location_region_id,

      -- attribution counts
      bizible_count_first_touch,
      bizible_count_lead_creation_touch,
      bizible_count_u_shaped,

      bizible_touchpoints.bizible_created_date

    FROM bizible_touchpoints
    LEFT JOIN account_dimensions
      ON bizible_touchpoints.bizible_account = account_dimensions.dim_crm_account_id
    LEFT JOIN crm_person
      ON bizible_touchpoints.bizible_person_id = crm_person.bizible_person_id
)

{{ dbt_audit(
    cte_ref="final_touchpoint",
    created_by="@mcooperDD",
    updated_by="@rkohnke",
    created_date="2021-01-21",
    updated_date="2024-01-31"
) }}
