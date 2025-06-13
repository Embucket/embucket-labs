WITH source AS (

    SELECT {{ nohash_sensitive_columns('marketo_activity_sync_lead_to_sfdc_source', 'marketo_activity_sync_lead_to_sfdc_id') }}
    FROM {{ ref('marketo_activity_sync_lead_to_sfdc_source') }}

)

SELECT *
FROM source
