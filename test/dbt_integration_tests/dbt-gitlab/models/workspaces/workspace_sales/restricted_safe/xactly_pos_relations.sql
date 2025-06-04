WITH source AS (

    SELECT {{ hash_sensitive_columns('xactly_pos_relations_source') }}
    FROM {{ ref('xactly_pos_relations_source') }}

)

SELECT *
FROM source
