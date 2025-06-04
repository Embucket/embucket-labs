{{ simple_cte([
      ('dim_date', 'dim_date'),
      ('sheetload_sales_targets_source', 'sheetload_sales_targets_source')
])}}

, fiscal_months AS (

    SELECT DISTINCT
      fiscal_year,
      first_day_of_month
    FROM dim_date

), final AS (

    SELECT
      CASE WHEN sheetload_sales_targets_source.kpi_name = 'Net ARR Company'
        THEN 'Net ARR'
      WHEN fiscal_months.fiscal_year = 2024 AND sheetload_sales_targets_source.kpi_name = 'Net ARR'
        THEN 'Net ARR - Stretch' 
      ELSE sheetload_sales_targets_source.kpi_name
      END                                                      AS kpi_name,
      sheetload_sales_targets_source.month,
      sheetload_sales_targets_source.sales_qualified_source,
      sheetload_sales_targets_source.order_type,
      sheetload_sales_targets_source.area,
      sheetload_sales_targets_source.allocated_target,
      sheetload_sales_targets_source.user_segment,
      sheetload_sales_targets_source.user_geo,
      IFF(LOWER(sheetload_sales_targets_source.user_segment) = LOWER('pubsec'),CONCAT(sheetload_sales_targets_source.user_geo, '-', UPPER(sheetload_sales_targets_source.user_segment)), sheetload_sales_targets_source.user_geo) AS user_geo_pubsec_segment,
      sheetload_sales_targets_source.user_region,
      sheetload_sales_targets_source.user_area,
      sheetload_sales_targets_source.user_business_unit,
      sheetload_sales_targets_source.user_role_name,
      sheetload_sales_targets_source.role_level_1,
      sheetload_sales_targets_source.role_level_2,
      sheetload_sales_targets_source.role_level_3,
      sheetload_sales_targets_source.role_level_4,
      sheetload_sales_targets_source.role_level_5,
      CASE
        WHEN fiscal_months.fiscal_year = 2024 AND LOWER(sheetload_sales_targets_source.user_business_unit) = 'comm'
          THEN CONCAT(
                      UPPER(sheetload_sales_targets_source.user_business_unit), 
                      '-',
                      UPPER(sheetload_sales_targets_source.user_geo), 
                      '-',
                      UPPER(sheetload_sales_targets_source.user_segment), 
                      '-',
                      UPPER(sheetload_sales_targets_source.user_region), 
                      '-',
                      UPPER(sheetload_sales_targets_source.user_area),
                      '-',
                      fiscal_months.fiscal_year
                      )
        WHEN fiscal_months.fiscal_year = 2024 AND LOWER(sheetload_sales_targets_source.user_business_unit) = 'entg'
          THEN CONCAT(
                      UPPER(sheetload_sales_targets_source.user_business_unit), 
                      '-',
                      UPPER(sheetload_sales_targets_source.user_geo), 
                      '-',
                      UPPER(sheetload_sales_targets_source.user_region), 
                      '-',
                      UPPER(sheetload_sales_targets_source.user_area), 
                      '-',
                      UPPER(sheetload_sales_targets_source.user_segment),
                      '-',
                      fiscal_months.fiscal_year
                      )
        WHEN fiscal_months.fiscal_year = 2024 
          AND (sheetload_sales_targets_source.user_business_unit IS NOT NULL AND LOWER(sheetload_sales_targets_source.user_business_unit) NOT IN ('comm', 'entg')) -- account for non-sales reps
          THEN CONCAT(
                      UPPER(sheetload_sales_targets_source.user_business_unit), 
                      '-',
                      UPPER(sheetload_sales_targets_source.user_segment), 
                      '-',
                      UPPER(sheetload_sales_targets_source.user_geo), 
                      '-',
                      UPPER(sheetload_sales_targets_source.user_region), 
                      '-',
                      UPPER(sheetload_sales_targets_source.user_area),
                      '-',
                      fiscal_months.fiscal_year
                      )
        WHEN fiscal_months.fiscal_year = 2024 AND sheetload_sales_targets_source.user_business_unit IS NULL -- account for nulls/possible data issues
          THEN CONCAT(
                      UPPER(sheetload_sales_targets_source.user_segment), 
                      '-',
                      UPPER(sheetload_sales_targets_source.user_geo), 
                      '-',
                      UPPER(sheetload_sales_targets_source.user_region), 
                      '-',
                      UPPER(sheetload_sales_targets_source.user_area),
                      '-',
                      fiscal_months.fiscal_year
                      )
        WHEN fiscal_months.fiscal_year >= 2025
          THEN CONCAT( -- some targets don't use the role hierarchy so we still need to generate a geo key when role_name is null.
                    COALESCE(
                    UPPER(sheetload_sales_targets_source.user_role_name),
                    CONCAT( 
                        UPPER(sheetload_sales_targets_source.user_segment),
                        '-',
                        UPPER(sheetload_sales_targets_source.user_geo), 
                        '-',
                        UPPER(sheetload_sales_targets_source.user_region),
                        '-',
                        UPPER(sheetload_sales_targets_source.user_area))
                        ),
                    '-',
                    fiscal_months.fiscal_year
                    )
        END                                                                                                                         AS dim_crm_user_hierarchy_sk,
        fiscal_months.fiscal_year,
        fiscal_months.first_day_of_month
    FROM sheetload_sales_targets_source
    INNER JOIN fiscal_months
      ON sheetload_sales_targets_source.month = fiscal_months.first_day_of_month

)

SELECT *
FROM final
