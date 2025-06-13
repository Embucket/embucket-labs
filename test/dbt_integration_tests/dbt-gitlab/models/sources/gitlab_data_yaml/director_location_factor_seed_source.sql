WITH source AS (

    SELECT *
    FROM {{ ref('director_location_factors') }}

), formated AS (

    SELECT
      country::VARCHAR                                   AS country, 
      locality::VARCHAR                                  AS locality, 
      factor::NUMBER(6, 3)                               AS factor, 
      valid_from::DATE                                   AS valid_from, 
      COALESCE(valid_to, {{ var('tomorrow') }})::DATE    AS valid_to, 
      is_current::BOOLEAN                                AS is_current
    FROM source

)

SELECT *
FROM formated