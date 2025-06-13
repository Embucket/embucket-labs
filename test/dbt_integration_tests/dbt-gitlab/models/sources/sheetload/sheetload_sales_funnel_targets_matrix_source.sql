{{ config(
    tags=["mnpi"]
) }}

WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_funnel_targets_matrix') }}

), renamed AS (

    SELECT
      kpi_name::VARCHAR                                                         AS kpi_name,
      month::VARCHAR                                                            AS month,
      {{ sales_qualified_source_cleaning('opportunity_source') }}::VARCHAR      AS opportunity_source,
      order_type::VARCHAR                                                       AS order_type,
      area::VARCHAR                                                             AS area,
      REPLACE(allocated_target, ',', '')::FLOAT                                 AS allocated_target,
      user_segment::VARCHAR                                                     AS user_segment,
      user_geo::VARCHAR                                                         AS user_geo,
      user_region::VARCHAR                                                      AS user_region,
      user_area::VARCHAR                                                        AS user_area,
      user_business_unit::VARCHAR                                               AS user_business_unit,
      user_role_type::VARCHAR                                                   AS user_role_type,
      TO_TIMESTAMP(TO_NUMERIC("_UPDATED_AT"))::TIMESTAMP                        AS last_updated_at
    FROM source

)

SELECT *
FROM renamed

UNION

-- Added new logo KPI so it is easier to relate fct_sales_funnel_target_daily and fct_sales_funnel_actual
-- This is because for the actual values there are two flags, one for Deals and another for New Logos
-- Issue that introduced this methodology: https://gitlab.com/gitlab-data/analytics/-/issues/18838
SELECT
    'New Logos' AS kpi_name,
    month,
    opportunity_source,
    order_type,
    area,
    allocated_target,
    user_segment,
    user_geo,
    user_region,
    user_area,
    user_business_unit,
    user_role_type,
    last_updated_at
FROM renamed
WHERE kpi_name = 'Deals'
    AND order_type = '1. New - First Order'
