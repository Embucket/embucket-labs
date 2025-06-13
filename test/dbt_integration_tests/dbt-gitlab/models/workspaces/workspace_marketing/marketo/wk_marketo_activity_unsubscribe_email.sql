WITH source AS (

    SELECT {{ hash_sensitive_columns('marketo_activity_unsubscribe_email_source') }}
    FROM {{ ref('marketo_activity_unsubscribe_email_source') }}

)

SELECT *
FROM source
