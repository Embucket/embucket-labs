WITH location_country AS (

    SELECT

      dim_location_country_id,
      dim_location_region_id,
      location_region_name_map,
      country_name,
      iso_2_country_code,
      iso_3_country_code,
      continent_name,
      is_in_european_union

    FROM {{ ref('prep_location_country') }}

)

{{ dbt_audit(
    cte_ref="location_country",
    created_by="@m_walker",
    updated_by="@mcooperDD",
    created_date="2020-08-25",
    updated_date="2020-01-28"
) }}
