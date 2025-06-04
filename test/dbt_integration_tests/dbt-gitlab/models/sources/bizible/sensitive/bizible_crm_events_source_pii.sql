WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_crm_events_source', 'crm_event_id') }}
    FROM {{ ref('bizible_crm_events_source') }}

)

SELECT *
FROM source