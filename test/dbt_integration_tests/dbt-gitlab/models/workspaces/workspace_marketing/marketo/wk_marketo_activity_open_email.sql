WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_open_email_source') }}

)

SELECT *
FROM source