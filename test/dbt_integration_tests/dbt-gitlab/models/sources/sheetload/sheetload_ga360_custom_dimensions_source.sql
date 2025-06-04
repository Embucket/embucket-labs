 WITH source AS (

        SELECT 
            dimension_name::VARCHAR AS dimension_name,
            dimension_index::NUMBER AS dimension_index,
            dimension_scope::VARCHAR AS dimension_scope
        FROM {{ source('sheetload','ga360_customdimensions') }}

)

SELECT * 
FROM source
