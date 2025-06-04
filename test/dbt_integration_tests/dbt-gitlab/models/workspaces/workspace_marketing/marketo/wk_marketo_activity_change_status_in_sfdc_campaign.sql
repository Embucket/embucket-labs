WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_change_status_in_sfdc_campaign_source') }}

)

SELECT *
FROM source