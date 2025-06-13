WITH source AS (

    SELECT *
    FROM {{ref('xactly_credit_type_source')}}

)

SELECT *
FROM source