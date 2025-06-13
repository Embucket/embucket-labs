WITH source AS (

    SELECT *
    FROM {{ref('xactly_period_type_source')}}

)

SELECT *
FROM source