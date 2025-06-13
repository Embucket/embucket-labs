WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_sync_lead_to_sfdc_source_pii') }}

)

SELECT *
FROM source