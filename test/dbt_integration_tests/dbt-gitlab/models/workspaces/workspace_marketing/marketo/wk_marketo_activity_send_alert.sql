WITH source AS (

    SELECT {{ hash_sensitive_columns('marketo_activity_send_alert_source') }}
    FROM {{ ref('marketo_activity_send_alert_source') }}

)

SELECT *
FROM source