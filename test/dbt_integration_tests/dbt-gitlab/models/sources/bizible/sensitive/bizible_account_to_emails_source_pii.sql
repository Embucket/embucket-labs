WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_account_to_emails_source', 'account_to_email_id') }}
    FROM {{ ref('bizible_account_to_emails_source') }}

)

SELECT *
FROM source