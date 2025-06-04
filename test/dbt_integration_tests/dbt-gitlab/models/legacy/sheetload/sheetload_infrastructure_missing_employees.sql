WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_infrastructure_missing_employees_source') }}

)

SELECT *
FROM source
