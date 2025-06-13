WITH source AS (

    SELECT *
    FROM {{ref('xactly_credit_totals_source')}}

)

SELECT *
FROM source