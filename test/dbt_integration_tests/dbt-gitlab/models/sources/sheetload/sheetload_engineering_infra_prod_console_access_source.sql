WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','engineering_infra_prod_console_access') }}
)

SELECT *
FROM source
