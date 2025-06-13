WITH source AS (

    SELECT {{ nohash_sensitive_columns('marketo_activity_send_alert_source', 'marketo_activity_send_alert_id') }}
    FROM {{ ref('marketo_activity_send_alert_source') }}

)

SELECT *
FROM source
