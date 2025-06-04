WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_stage_definitions_source') }}
    FROM {{ ref('bizible_stage_definitions_source') }}

)

SELECT *
FROM source