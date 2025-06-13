WITH source AS (

    SELECT {{ hash_sensitive_columns('marketo_activity_email_bounced_soft_source') }}
    FROM {{ ref('marketo_activity_email_bounced_soft_source') }}

)

SELECT *
FROM source