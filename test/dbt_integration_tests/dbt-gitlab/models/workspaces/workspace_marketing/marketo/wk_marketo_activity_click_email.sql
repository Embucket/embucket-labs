WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_click_email_source') }}

)

SELECT *
FROM source