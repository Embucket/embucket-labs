WITH source AS (

    SELECT *
    FROM {{ref('xactly_pos_hierarchy_type_source')}}

)

SELECT *
FROM source