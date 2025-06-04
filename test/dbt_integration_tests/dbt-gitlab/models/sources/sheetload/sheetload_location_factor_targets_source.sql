WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'location_factor_targets') }}

), renamed AS (

    SELECT 
      department::VARCHAR AS department,
      target::FLOAT       AS location_factor_target
    FROM source

)

SELECT *
FROM renamed