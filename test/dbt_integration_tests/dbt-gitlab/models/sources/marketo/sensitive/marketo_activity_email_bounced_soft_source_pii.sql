WITH source AS (

    SELECT {{ nohash_sensitive_columns('marketo_activity_email_bounced_soft_source', 'marketo_activity_email_bounced_soft_id') }}
    FROM {{ ref('marketo_activity_email_bounced_soft_source') }}

)

SELECT *
FROM source