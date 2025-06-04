WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_days_to_close_source') }}

)

SELECT *
FROM source