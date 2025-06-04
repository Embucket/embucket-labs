WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_engineering_infra_prod_console_access_source') }}

)

SELECT *
FROM source

