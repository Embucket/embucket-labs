WITH source AS (
      /* 
       Selecting only non error results as there will be nothing to 
       flatten  if there was an error
       */

    SELECT
      jsontext,
      uploaded_at,
      LEAD(uploaded_at,1) OVER (ORDER BY uploaded_at) AS lead_uploaded_at,
      MAX(uploaded_at) OVER ()                        AS max_uploaded_at
    FROM {{ source('gitlab_data_yaml', 'location_factors') }} 
    WHERE NOT CONTAINS(jsontext, 'Server Error')
      
), grouped AS (
      /*
      Reducing to only the changed values to reduce processing load
      and create a contiguious range of data.
      */
      
    SELECT DISTINCT
      jsontext,
      MIN(uploaded_at) OVER (PARTITION BY jsontext)      AS valid_from,
      MAX(lead_uploaded_at) OVER (PARTITION BY jsontext) AS valid_to,
      IFF(valid_to = max_uploaded_at, TRUE, FALSE)       AS is_current
    FROM source

), level_1 AS (

    SELECT
      valid_from,
      valid_to,
      is_current,
      level_1.value['area']::VARCHAR                AS area_level_1,
      level_1.value['country']::VARCHAR             AS country_level_1,
      level_1.value['locationFactor']::NUMBER(6, 3) AS locationfactor_level_1,
      level_1.value['factor']::NUMBER(6, 3)         AS factor_level_1,
      level_1.value['metro_areas']::VARIANT         AS metro_areas_level_1,
      level_1.value['states_or_provinces']::VARIANT AS states_or_provinces_level_1
    FROM grouped
    INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(jsontext), OUTER => TRUE) AS level_1

), level_1_metro_areas AS (

    SELECT
      level_1.*,
      level_1_metro_areas.value['name']::VARCHAR         AS metro_areas_name_level_2,
      level_1_metro_areas.value['factor']::NUMBER(6, 3)  AS metro_areas_factor_level_2,
      level_1_metro_areas.value['sub_location']::VARCHAR AS metro_areas_sub_location_level_2
    FROM level_1
    INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(metro_areas_level_1), OUTER => TRUE) AS level_1_metro_areas
    UNION ALL
    -- For the country level override when there is also a metro area
    SELECT
      level_1.*,
      NULL AS metro_areas_name_level_2,
      NULL AS metro_areas_factor_level_2,
      NULL AS metro_areas_sub_location_level_2
    FROM level_1
    WHERE factor_level_1 IS NOT NULL
      AND metro_areas_level_1 IS NOT NULL

), level_1_states_or_provinces AS (

    SELECT
      level_1_metro_areas.*,
      level_1_states_or_provinces.value['name']::VARCHAR        AS states_or_provinces_name_level_2,
      level_1_states_or_provinces.value['factor']::NUMBER(6, 3) AS states_or_provinces_factor_level_2,
      level_1_states_or_provinces.value['metro_areas']::VARIANT AS states_or_provinces_metro_areas_level_2
    FROM level_1_metro_areas
    INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(states_or_provinces_level_1), OUTER =>
                               TRUE) AS level_1_states_or_provinces

), level_2_states_or_provinces_metro_areas AS (

    SELECT
      level_1_states_or_provinces.*,
      level_2_states_or_provinces_metro_areas.value['name']::VARCHAR        AS states_or_provinces_metro_areas_name_level_2,
      level_2_states_or_provinces_metro_areas.value['factor']::NUMBER(6, 3) AS states_or_provinces_metro_areas_factor_level_2
    FROM level_1_states_or_provinces
    INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(states_or_provinces_metro_areas_level_2), OUTER =>
                                 TRUE) AS level_2_states_or_provinces_metro_areas

)

  SELECT
    valid_from,
    valid_to,
    is_current,
    area_level_1,
    country_level_1,
    locationfactor_level_1,
    factor_level_1,
    metro_areas_name_level_2,
    metro_areas_factor_level_2,
    metro_areas_sub_location_level_2,
    states_or_provinces_name_level_2,
    states_or_provinces_factor_level_2,
    states_or_provinces_metro_areas_name_level_2,
    states_or_provinces_metro_areas_factor_level_2
  FROM level_2_states_or_provinces_metro_areas