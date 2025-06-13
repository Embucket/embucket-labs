WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_lead_stage_transitions_source', 'lead_stage_transition_id') }}
    FROM {{ ref('bizible_lead_stage_transitions_source') }}

)

SELECT *
FROM source