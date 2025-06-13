WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_reconciliations_source') }}

)

SELECT *
FROM source
