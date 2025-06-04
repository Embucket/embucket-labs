WITH source AS (

    SELECT *
    FROM {{ref('xactly_credit_held_source')}}

)

SELECT *
FROM source