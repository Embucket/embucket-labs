  WITH source AS (

    SELECT *
    FROM {{ ref('location_factors_yaml_flatten_source') }}

), organized AS (

    SELECT
      valid_from,
      valid_to,
      is_current,
      country_level_1                                                                     AS country,
      -- Idenitfy type based on where the data is from the flattend structure.
      CASE
        WHEN area_level_1 IS NOT NULL THEN 'type_1'
        WHEN states_or_provinces_metro_areas_name_level_2 IS NOT NULL THEN 'type_2'
        WHEN metro_areas_sub_location_level_2 IS NOT NULL THEN 'type_3'
        WHEN states_or_provinces_metro_areas_name_level_2 IS NULL AND states_or_provinces_name_level_2 IS NOT NULL
          THEN 'type_4'
        WHEN metro_areas_sub_location_level_2 IS NULL AND metro_areas_name_level_2 IS NOT NULL THEN 'type_5'
        ELSE 'type_6'
      END                                                                                 AS type,
      CASE type
        WHEN 'type_1' THEN area_level_1
        WHEN 'type_2' THEN states_or_provinces_metro_areas_name_level_2 || ', ' || states_or_provinces_name_level_2
        WHEN 'type_3' THEN metro_areas_name_level_2 || ', ' || metro_areas_sub_location_level_2
        WHEN 'type_4' THEN states_or_provinces_name_level_2
        WHEN 'type_5' THEN metro_areas_name_level_2
        ELSE NULL
      END                                                                                 AS area_raw,
      LISTAGG(DISTINCT area_raw, ',') OVER (PARTITION BY country)                         AS area_list,
      -- Prefer All to Everyere else for areas without as it is what is currentl added in the application.
      CASE
        WHEN CONTAINS(area_list, 'All') THEN 'All'
        WHEN CONTAINS(area_list, 'Everywhere else') THEN 'Everywhere else'
        ELSE NULL
      END                                                                                 AS other_area,
      -- Cleaning basic spelling erros in the source data and apply a derived area prefix if there is none.
      CASE
        WHEN area_raw IS NULL THEN other_area
        WHEN contains(area_raw,'Zurig') THEN regexp_replace(area_raw,'Zurig','Zurich')
        WHEN contains(area_raw,'Edinbugh') THEN regexp_replace(area_raw,'Edinbugh','Edinburgh')
        ELSE area_raw
      END                                                                                 AS area_clean,
      -- Adjusting factor to match what is found in the geozone source data
      COALESCE(states_or_provinces_metro_areas_factor_level_2, states_or_provinces_factor_level_2,
               metro_areas_factor_level_2, factor_level_1, locationfactor_level_1) * 0.01 AS factor
    FROM source

), grouping AS (

    SELECT
      country,
      area_clean                                                                                    AS area,
      area || ', ' || country                                                                       AS locality,
      factor,
      valid_from,
      valid_to,
      is_current,
      /* Filling in NULLs with a value for the inequality check in the next step of the gaps and islands problem
      (finding groups based on when the factor changes and not just the value of the factor)
      */
      LAG(factor, 1, 0) OVER (PARTITION BY locality ORDER BY valid_from)                            AS lag_factor,
      CONDITIONAL_TRUE_EVENT(factor != lag_factor) OVER (PARTITION BY locality ORDER BY valid_from) AS locality_group,
      LEAD(valid_from,1) OVER (PARTITION BY locality ORDER BY valid_from)                           AS next_entry
    FROM organized

), final AS (

    SELECT DISTINCT
      country                                                                                           AS location_factor_country,
      area                                                                                              AS location_factor_area,
      locality,
      factor                                                                                            AS location_factor,
      MIN(valid_from) OVER (PARTITION BY locality,locality_group)::DATE                                 AS first_file_date,
      MAX(valid_to) OVER (PARTITION BY locality,locality_group)::DATE                                   AS last_file_date,
      -- Fixed date represents when location factor becan to be collected in source data.
      IFF(locality_group = 1, LEAST('2020-03-24',first_file_date),first_file_date)                      AS valid_from,
      MAX(COALESCE(next_entry,{{ var('tomorrow') }})) OVER (PARTITION BY locality,locality_group)::DATE AS valid_to,
      BOOLOR_AGG(is_current) OVER (PARTITION BY locality,locality_group)                                AS is_current_file
    FROM grouping
    WHERE factor IS NOT NULL

)

SELECT *
FROM final
