WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_placements_source') }}
    FROM {{ ref('bizible_placements_source') }}

)

SELECT *
FROM source