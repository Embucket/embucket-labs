WITH source AS (

    SELECT *
    FROM {{ref('xactly_position_source')}}

)

SELECT *
FROM source