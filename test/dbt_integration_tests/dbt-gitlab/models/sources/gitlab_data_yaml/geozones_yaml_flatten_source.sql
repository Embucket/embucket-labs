WITH source AS (

    SELECT
      jsontext,
      uploaded_at,
      LEAD(uploaded_at,1) OVER (ORDER BY uploaded_at) AS lead_uploaded_at,
      MAX(uploaded_at) OVER ()                        AS max_uploaded_at
    FROM {{ source('gitlab_data_yaml', 'geo_zones') }}

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

), geozones AS (

      SELECT
        valid_from,
        valid_to,
        is_current,
        geozones.value['title']::VARCHAR               AS geozone_title,
        geozones.value['factor']::NUMBER(6, 3)         AS geozone_factor,
        geozones.value['countries']::VARIANT           AS geozone_countries,
        geozones.value['states_or_provinces']::VARIANT AS geozone_states_or_provinces
      FROM grouped
      INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), OUTER => TRUE) AS geozones

), countries AS (

      SELECT
        *,
        countries.value::VARCHAR AS country
      FROM geozones
      INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(geozone_countries), OUTER => TRUE) AS countries

), states_or_provinces AS (

      SELECT
        *,
        states_or_provinces.value::VARCHAR AS state_or_province
      FROM countries
      INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(geozone_states_or_provinces), OUTER => TRUE) AS states_or_provinces

)

  SELECT
    valid_from,
    valid_to,
    is_current,
    geozone_title,
    geozone_factor,
    country,
    state_or_province
  FROM states_or_provinces