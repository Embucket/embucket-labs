WITH source AS (

    SELECT *
    FROM {{ref('xactly_user_source')}}

)

SELECT *
FROM source