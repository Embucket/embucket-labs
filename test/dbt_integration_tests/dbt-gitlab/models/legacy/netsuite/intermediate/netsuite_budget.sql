WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_budget_source') }}

)

SELECT *
FROM source
WHERE is_fivetran_deleted = FALSE