WITH source AS (

    SELECT
      jsontext,
      DATE_TRUNC(day, uploaded_at) AS uploaded_at,
      RANK() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'geo_zones') }}
    ORDER BY uploaded_at DESC

), intermediate AS (

    SELECT 
      geozones.value['title']::VARCHAR                                 AS geozone_title, 
      geozones.value['factor']::VARCHAR                                AS geozone_factor, 
      geozones.index                                                   AS geozone_index,
      additional_fields.key::VARCHAR                                   AS info_key,
      field_info.value::VARCHAR                                        AS field_value,
      IFF(uploaded_at = '2021-01-04', '2021-01-01', uploaded_at)       AS uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(null), OUTER => TRUE) geozones,
    LATERAL FLATTEN(INPUT => parse_json(null), OUTER => TRUE) additional_fields,
    TABLE(FLATTEN(input => additional_fields.value, RECURSIVE => TRUE))  field_info

), unioned AS (

    SELECT DISTINCT
      geozone_title,
      geozone_factor,
      'United States'              AS country,
      field_value                  AS state_or_province,
      uploaded_at
    FROM intermediate
    WHERE info_key IN ('states_or_provinces')

    UNION ALL 

    SELECT DISTINCT
      geozone_title,
      geozone_factor,
      field_value                 AS country,
      NULL                        AS state_or_province,
      uploaded_at
    FROM intermediate
    WHERE info_key IN ('countries')  
  
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['geozone_title', 'geozone_factor', 'country', 'state_or_province','geozone_factor']) }}        AS unique_key,
  unioned.*
FROM unioned
