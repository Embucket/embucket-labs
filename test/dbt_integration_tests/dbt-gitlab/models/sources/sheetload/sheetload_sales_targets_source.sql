        WITH source AS (

        SELECT * 
        FROM {{ source('sheetload','sales_targets') }}

), renamed AS (

        SELECT
          scenario::VARCHAR                                                         AS scenario,
          kpi_name::VARCHAR                                                         AS kpi_name,
          month::VARCHAR                                                            AS month,
          {{ sales_qualified_source_cleaning('sales_qualified_source') }}::VARCHAR  AS sales_qualified_source,
          alliance_partner::VARCHAR                                                 AS alliance_partner,
          partner_category::VARCHAR                                                 AS partner_category,
          order_type::VARCHAR                                                       AS order_type,
          area::VARCHAR                                                             AS area,
          user_segment::VARCHAR                                                     AS user_segment,
          user_geo::VARCHAR                                                         AS user_geo,
          user_region::VARCHAR                                                      AS user_region,
          user_area::VARCHAR                                                        AS user_area,
          user_business_unit::VARCHAR                                               AS user_business_unit,
          user_role_name::VARCHAR                                                   AS user_role_name,
          role_level_1::VARCHAR                                                     AS role_level_1,
          role_level_2::VARCHAR                                                     AS role_level_2,
          role_level_3::VARCHAR                                                     AS role_level_3,
          role_level_4::VARCHAR                                                     AS role_level_4,
          role_level_5::VARCHAR                                                     AS role_level_5,
          TRY_TO_NUMBER(REPLACE(allocated_target, ',', ''), 38, 20)::FLOAT          AS allocated_target
        FROM source
)

        SELECT *
        FROM renamed

        UNION

        -- Added new logo KPI so it is easier to relate fct_sales_funnel_target_daily and fct_sales_funnel_actual
        -- This is because for the actual values there are two flags, one for Deals and another for New Logos
        -- Issue that introduced this methodology: https://gitlab.com/gitlab-data/analytics/-/issues/18838
        SELECT
          scenario,
          'New Logos' AS kpi_name,
          month,
          sales_qualified_source,
          alliance_partner,
          partner_category,
          order_type,
          area,
          user_segment,
          user_geo,
          user_region,
          user_area,
          user_business_unit,
          user_role_name,
          role_level_1,
          role_level_2,
          role_level_3,
          role_level_4,
          role_level_5,
          allocated_target
        FROM renamed
        WHERE kpi_name = 'Deals'
            AND order_type = '1. New - First Order'