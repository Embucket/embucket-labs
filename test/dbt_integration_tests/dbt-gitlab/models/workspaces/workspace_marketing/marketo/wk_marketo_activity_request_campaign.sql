WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_request_campaign_source') }}

)

SELECT *
FROM source