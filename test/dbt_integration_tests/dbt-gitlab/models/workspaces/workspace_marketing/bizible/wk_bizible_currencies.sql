WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_currencies_source') }}
    FROM {{ ref('bizible_currencies_source') }}

)

SELECT *
FROM source