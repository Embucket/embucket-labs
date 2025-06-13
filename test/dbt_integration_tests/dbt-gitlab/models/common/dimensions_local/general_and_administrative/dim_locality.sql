{{ simple_cte([
    ('geozones', 'prep_geozone'),
    ('location_factor', 'prep_location_factor'),
    ('director_factors','director_location_factor_seed_source')
])}}
, geozone_titles AS (

    SELECT
      IFF(geozone_country = 'United States', geozone_country, NULL) AS country,
      geozone_locality,
      geozone_factor,
      MIN(valid_from)                                               AS valid_from,
      MAX(valid_to)                                                 AS valid_to,
      BOOLAND_AGG(is_current_file)                                  AS is_current_file
    FROM geozones
    GROUP BY 1, 2, 3

), geozone_countries AS (

    SELECT
      geozone_country,
      country_locality,
      geozone_factor,
      MIN(valid_from)                               AS valid_from,
      MAX(valid_to)                                 AS valid_to,
      BOOLAND_AGG(is_current_file)                  AS is_current_file
    FROM geozones
    GROUP BY 1, 2, 3

), null_entry AS (

    SELECT
      *
    FROM (VALUES ('unknown','unknown',-1,{{ var('infinity_past') }},{{ var('infinity_future') }},FALSE)) 
        AS null_entry (country, locality, factor, valid_from, valid_to)

), join_spine AS (

    SELECT DISTINCT
      location_factor_country AS country,
      valid_from
    FROM location_factor
    UNION
    SELECT DISTINCT
      geozone_country         AS country,
      valid_from
    FROM geozones

), combined AS (

    SELECT DISTINCT
      join_spine.country,
      COALESCE(location_factor.locality, geozone_countries.country_locality)          AS locality,
      COALESCE(location_factor.location_factor, geozone_countries.geozone_factor)     AS location_factor,
      COALESCE(location_factor.valid_from, geozone_countries.valid_from)              AS valid_from,
      COALESCE(location_factor.valid_to, geozone_countries.valid_to)                  AS valid_to,
      COALESCE(location_factor.is_current_file, geozone_countries.is_current_file)    AS is_current_source_file
    FROM join_spine
    LEFT JOIN geozone_countries
      ON geozone_countries.geozone_country = join_spine.country
      AND join_spine.valid_from >= geozone_countries.valid_from
      AND join_spine.valid_from < geozone_countries.valid_to
    LEFT JOIN location_factor
      ON location_factor.location_factor_country = join_spine.country
      AND join_spine.valid_from >= location_factor.valid_from
      AND join_spine.valid_from < location_factor.valid_to

    UNION

    SELECT *
    FROM geozone_titles

    UNION

    SELECT *
    FROM director_factors

    UNION 

    SELECT *
    FROM null_entry

), final AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['lower(locality)']) }} AS dim_locality_id,
      locality,
      location_factor,
      country                                            AS locality_country,
      valid_from,
      valid_to,
      is_current_source_file
    FROM combined

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@pempey",
    updated_by="@pempey",
    created_date="2021-12-21",
    updated_date="2021-12-21"
) }}