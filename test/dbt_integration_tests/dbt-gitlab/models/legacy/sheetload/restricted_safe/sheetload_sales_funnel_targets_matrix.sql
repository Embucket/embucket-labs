WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sales_funnel_targets_matrix_source') }}

), final AS (

    SELECT
      kpi_name,
      month,
      opportunity_source,
      order_type,
      area,
      allocated_target,
      user_segment,
      user_geo,
      user_region,
      user_area,
      user_business_unit
    FROM source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@michellecooper",
    created_date="2020-11-18",
    updated_date="2023-03-10"
) }}
