WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_headcount_source') }}

)

SELECT *
FROM source