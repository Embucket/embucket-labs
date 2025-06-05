WITH source AS (

    SELECT *
    FROM EMBUCKET.netsuite.netsuite_budget_source

)

SELECT *
FROM source
WHERE is_fivetran_deleted = FALSE