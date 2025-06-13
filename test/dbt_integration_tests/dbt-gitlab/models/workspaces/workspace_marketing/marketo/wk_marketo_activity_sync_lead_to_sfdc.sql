WITH source AS (

    SELECT {{ hash_sensitive_columns('marketo_activity_sync_lead_to_sfdc_source') }}
    FROM {{ ref('marketo_activity_sync_lead_to_sfdc_source') }}

)

SELECT *
FROM source