WITH source AS (

    SELECT *
    FROM {{ref('xactly_pos_hierarchy_source')}}

)

SELECT *
FROM source