WITH source AS (

        SELECT * 
        FROM {{ source('sheetload','sales_dev_role_hierarchy') }}

        )

SELECT * 
FROM source