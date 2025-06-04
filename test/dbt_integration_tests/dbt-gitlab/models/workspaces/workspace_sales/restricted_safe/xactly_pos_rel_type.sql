WITH source AS (

    SELECT *
    FROM {{ref('xactly_pos_rel_type_source')}}

)

SELECT *
FROM source