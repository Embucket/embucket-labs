WITH source AS (

    SELECT *,
      DATE_TRUNC('day', uploaded_at)::DATE            AS snapshot_date,
      RANK() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'location_factors') }}
    ORDER BY uploaded_at DESC
  
), original_yaml_file_format AS (

    SELECT
      d.value['area']::VARCHAR                  AS area,
      d.value['country']::VARCHAR               AS country,
      d.value['locationFactor']::FLOAT          AS location_factor,
      snapshot_date,
      rank
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d
    WHERE snapshot_date <= '2020-10-21'

), united_states AS (

    SELECT DISTINCT
      initial_unnest.value['country']::VARCHAR      AS country, 
      countries_info.value['name']::VARCHAR         AS location_state,
      metro_areas.value['name']::VARCHAR            AS metro_area,
      metro_areas.value['factor']::VARCHAR          AS location_factor,
      snapshot_date,
      rank
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) initial_unnest,
    LATERAL FLATTEN(INPUT => parse_json(initial_unnest.value), OUTER => TRUE) countries,
    TABLE(FLATTEN(input => countries.value, RECURSIVE => TRUE))  countries_info,
    TABLE(FLATTEN(input => countries_info.value, RECURSIVE => TRUE))  metro_areas
    WHERE snapshot_date >= '2021-01-04'
      AND countries_info.index IS NOT NULL
      AND metro_areas.index IS NOT NULL
      AND country = 'United States'

), other AS (

    SELECT DISTINCT
      initial_unnest.value['country']::VARCHAR      AS country, 
      countries_info.value['sub_location']::VARCHAR AS state,
      countries_info.value['name']::VARCHAR         AS metro_area,
      countries_info.value['factor']::VARCHAR       AS location_factor,
      snapshot_date,
      rank,
      countries_info.index                          AS country_info_index
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) initial_unnest,
    LATERAL FLATTEN(INPUT => parse_json(initial_unnest.value), OUTER => TRUE) countries,
    TABLE(FLATTEN(input => countries.value, RECURSIVE => TRUE))  countries_info
    WHERE snapshot_date >= '2021-01-04'
      AND countries_info.index IS NOT NULL
    
), countries_without_metro_areas AS (

    SELECT DISTINCT
      CASE
        WHEN initial_unnest.value['country']::VARCHAR = 'Israel' THEN 'All'
        WHEN initial_unnest.value['metro_areas']::VARCHAR IS NOT NULL THEN 'Everywhere else'
        ELSE 'All'
      END                                           AS area,
      initial_unnest.value['country']::VARCHAR      AS country, 
      initial_unnest.value['factor']::VARCHAR       AS location_factor,
      snapshot_date,
      rank
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) initial_unnest,
    LATERAL FLATTEN(INPUT => parse_json(initial_unnest.value), OUTER => TRUE) countries
    WHERE snapshot_date >= '2021-01-04'
      AND location_factor IS NOT NULL

), unioned AS (
  
    SELECT 
      area,
      country,
      location_factor,
      snapshot_date,
      rank
    FROM original_yaml_file_format

    UNION ALL

    SELECT
      metro_area || ', '|| location_state                AS area,
      country,
      location_factor,
      snapshot_date,
      rank
    FROM united_states

    UNION ALL 
  
    SELECT
      metro_area || IFF( state is null,'',', ' || state ) AS metro_area,
      country,
      location_factor,
      snapshot_date,
      rank
    FROM other
    WHERE country != 'United States'

    UNION  ALL

    SELECT
      area AS metro_area,
      country,
      location_factor,
      snapshot_date,
      rank
    FROM countries_without_metro_areas

    UNION ALL 

    SELECT
      metro_area,
      country,
      location_factor,
      snapshot_date,
      rank
    FROM other
    WHERE country = 'United States'
      AND location_factor IS NOT NULL
      AND metro_area IN ('Hawaii','Washington DC')
    ---this is to capture data points with just metro area  

)

SELECT *
FROM unioned
WHERE rank = 1
