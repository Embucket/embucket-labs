WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sisense_users') }}

), renamed as (

    SELECT
      id::VARCHAR                            AS id,
      first_name::VARCHAR                    AS first_name,
      last_name::VARCHAR                     AS last_name,
      email_address::VARCHAR                 AS email_address
      
    FROM source
)

SELECT *
FROM renamed
