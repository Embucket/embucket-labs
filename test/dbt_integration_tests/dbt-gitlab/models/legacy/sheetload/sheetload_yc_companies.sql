WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_yc_companies_source') }}

)

SELECT *
FROM source