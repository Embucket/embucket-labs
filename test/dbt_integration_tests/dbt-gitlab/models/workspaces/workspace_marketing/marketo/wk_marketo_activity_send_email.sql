WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_send_email_source') }}

)

SELECT *
FROM source