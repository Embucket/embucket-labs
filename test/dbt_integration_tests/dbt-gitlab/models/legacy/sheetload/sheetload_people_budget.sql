WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_people_budget_source') }}

)

SELECT *
FROM source
