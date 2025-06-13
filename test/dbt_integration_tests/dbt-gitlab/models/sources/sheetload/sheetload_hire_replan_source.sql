WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','hire_replan') }}

) 

SELECT *
FROM source
