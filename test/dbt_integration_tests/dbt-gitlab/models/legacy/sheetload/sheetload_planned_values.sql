WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_planned_values_source') }}

)

SELECT *
FROM source