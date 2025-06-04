WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_scalable_employment_values_source') }}

)

SELECT *
FROM source