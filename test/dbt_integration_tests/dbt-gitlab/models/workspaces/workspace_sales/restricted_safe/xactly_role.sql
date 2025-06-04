WITH source AS (

    SELECT *
    FROM {{ref('xactly_role_source')}}

)

SELECT *
FROM source