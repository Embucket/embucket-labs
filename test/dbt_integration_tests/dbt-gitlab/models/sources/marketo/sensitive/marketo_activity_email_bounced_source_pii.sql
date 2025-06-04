WITH source AS (

    SELECT {{ nohash_sensitive_columns('marketo_activity_email_bounced_source', 'marketo_activity_email_bounced_id') }}
    FROM {{ ref('marketo_activity_email_bounced_source') }}

)

SELECT *
FROM source