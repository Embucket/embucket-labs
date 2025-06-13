WITH source AS (

    SELECT *
    FROM {{ ref('bizible_opp_stage_transitions_source_pii') }}

)

SELECT *
FROM source