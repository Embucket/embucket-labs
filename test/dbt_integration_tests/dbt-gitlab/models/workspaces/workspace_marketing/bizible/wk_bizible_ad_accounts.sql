WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_ad_accounts_source') }}
    FROM {{ ref('bizible_ad_accounts_source') }}

)

SELECT *
FROM source