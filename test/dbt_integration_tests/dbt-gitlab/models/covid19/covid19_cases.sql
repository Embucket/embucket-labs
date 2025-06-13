WITH source AS (

    SELECT *
    FROM {{ source('covid19', 'cases') }}

), renamed AS (

    SELECT 
      country_region::VARCHAR       AS country_region,
      province_state::VARCHAR       AS province_state,
      date::DATE                    AS date,
      case_type::VARCHAR            AS case_type,
      cases::NUMBER                 AS case_count,
      long::FLOAT                   AS longitude,
      lat::FLOAT                    AS latitude,
      difference::NUMBER            AS case_count_change,
      last_updated_date::TIMESTAMP  AS last_updated_date
    FROM source

)

SELECT *
FROM renamed
