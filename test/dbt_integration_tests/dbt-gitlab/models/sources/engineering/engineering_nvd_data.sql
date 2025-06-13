WITH source AS (

    SELECT *
    FROM {{ source('engineering', 'nvd_data') }}

), renamed AS (

    SELECT
      "0"::NUMBER       AS year,
      "1"::NUMBER       AS count
    FROM source

)

SELECT *
FROM renamed
