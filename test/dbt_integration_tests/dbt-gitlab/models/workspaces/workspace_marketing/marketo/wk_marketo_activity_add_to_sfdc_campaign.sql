WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_add_to_sfdc_campaign_source') }}

)

SELECT *
FROM source