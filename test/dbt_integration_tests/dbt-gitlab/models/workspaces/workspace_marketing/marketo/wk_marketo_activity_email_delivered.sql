WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_email_delivered_source') }}

)

SELECT *
FROM source