WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_opp_stage_transitions_source') }}
    FROM {{ ref('bizible_opp_stage_transitions_source') }}

)

SELECT *
FROM source