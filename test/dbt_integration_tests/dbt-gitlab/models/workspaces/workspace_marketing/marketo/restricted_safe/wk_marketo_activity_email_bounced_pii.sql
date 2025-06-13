WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_email_bounced_source_pii') }}

)

SELECT *
FROM source