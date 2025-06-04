WITH source AS (

    SELECT {{ hash_sensitive_columns('xactly_pos_hierarchy_hist_source') }}
    FROM {{ ref('xactly_pos_hierarchy_hist_source') }}

)

SELECT *
FROM source
