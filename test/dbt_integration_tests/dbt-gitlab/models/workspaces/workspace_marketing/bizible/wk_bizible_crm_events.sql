WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_crm_events_source') }}
    FROM {{ ref('bizible_crm_events_source') }}

)

SELECT *
FROM source