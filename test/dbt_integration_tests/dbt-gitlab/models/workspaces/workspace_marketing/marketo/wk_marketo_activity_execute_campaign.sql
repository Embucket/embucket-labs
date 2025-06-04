WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_execute_campaign_source') }}

)

SELECT *
FROM source

