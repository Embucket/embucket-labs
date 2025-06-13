WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','hostnames_to_domains') }}

)

SELECT * 
FROM source
