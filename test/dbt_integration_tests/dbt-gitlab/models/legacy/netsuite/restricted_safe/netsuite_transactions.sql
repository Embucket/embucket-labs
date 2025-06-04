WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_transactions_source') }}

)

SELECT *
FROM source