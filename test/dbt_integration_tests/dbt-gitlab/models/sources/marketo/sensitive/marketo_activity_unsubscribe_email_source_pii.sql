WITH source AS (

    SELECT {{ nohash_sensitive_columns('marketo_activity_unsubscribe_email_source', 'marketo_activity_unsubscribe_email_id') }}
    FROM {{ ref('marketo_activity_unsubscribe_email_source') }}

)

SELECT *
FROM source
