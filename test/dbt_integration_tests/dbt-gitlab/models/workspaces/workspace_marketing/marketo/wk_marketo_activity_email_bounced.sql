WITH source AS (

    SELECT {{ hash_sensitive_columns('marketo_activity_email_bounced_source') }}
    FROM {{ ref('marketo_activity_email_bounced_source') }}

)

SELECT *
FROM source