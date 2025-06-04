WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_accounts_source') }}

)

SELECT *
FROM source