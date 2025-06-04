WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_opp_stage_transitions_source', 'opp_stage_transition_id') }}
    FROM {{ ref('bizible_opp_stage_transitions_source') }}

)

SELECT *
FROM source