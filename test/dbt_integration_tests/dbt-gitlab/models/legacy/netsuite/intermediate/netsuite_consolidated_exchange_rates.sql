WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_consolidated_exchange_rates_source') }}

)

SELECT *
FROM source
WHERE is_fivetran_deleted = FALSE