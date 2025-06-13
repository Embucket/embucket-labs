WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_account_to_emails_source') }}
    FROM {{ ref('bizible_account_to_emails_source') }}

)

SELECT *
FROM source