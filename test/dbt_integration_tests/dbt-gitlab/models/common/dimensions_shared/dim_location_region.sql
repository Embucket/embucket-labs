WITH location_region AS (

    SELECT
      dim_location_region_id,
      location_region_name
    FROM {{ ref('prep_location_region') }}
)

{{ dbt_audit(
    cte_ref="location_region",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-12-29",
    updated_date="2020-12-29"
) }}
