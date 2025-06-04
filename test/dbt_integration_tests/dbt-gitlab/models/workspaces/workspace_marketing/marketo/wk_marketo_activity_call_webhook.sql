WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_call_webhook_source') }}

)

SELECT *
FROM source