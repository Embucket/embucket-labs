WITH source AS (

    SELECT *
    FROM {{ ref('bizible_accounts_source_pii') }}

)

SELECT *
FROM source