WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_segments_source') }}
    FROM {{ ref('bizible_segments_source') }}

)

SELECT *
FROM source