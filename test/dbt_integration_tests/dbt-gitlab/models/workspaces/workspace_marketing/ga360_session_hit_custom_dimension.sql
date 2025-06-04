WITH source AS (

    SELECT *
    FROM {{ref('ga360_session_hit_custom_dimension_source')}}

)

SELECT *
FROM source