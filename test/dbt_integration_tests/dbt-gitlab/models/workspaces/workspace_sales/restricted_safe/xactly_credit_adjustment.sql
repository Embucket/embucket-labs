WITH source AS (

    SELECT *
    FROM {{ref('xactly_credit_adjustment_source')}}

)

SELECT *
FROM source