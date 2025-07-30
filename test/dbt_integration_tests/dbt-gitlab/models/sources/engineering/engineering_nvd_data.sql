WITH source AS (

    SELECT *
    FROM {{ source('engineering', 'nvd_data') }}

), renamed AS (

    SELECT
      year::NUMBER       AS year,
      cnt::NUMBER       AS count
    FROM source

)

SELECT *
FROM renamed
