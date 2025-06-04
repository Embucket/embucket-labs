WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_remove_from_sfdc_campaign_source') }}

)

SELECT *
FROM source