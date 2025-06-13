WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_accounts_source', 'account_id') }}
    FROM {{ ref('bizible_accounts_source') }}

)

SELECT *
FROM source