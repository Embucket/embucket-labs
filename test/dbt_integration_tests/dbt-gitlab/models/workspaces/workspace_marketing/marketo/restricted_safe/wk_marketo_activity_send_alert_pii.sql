WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_send_alert_source_pii') }}

)

SELECT *
FROM source