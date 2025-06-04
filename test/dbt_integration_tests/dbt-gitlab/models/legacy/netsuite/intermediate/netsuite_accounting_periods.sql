WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_accounting_periods_source') }}

)

SELECT *
FROM source
