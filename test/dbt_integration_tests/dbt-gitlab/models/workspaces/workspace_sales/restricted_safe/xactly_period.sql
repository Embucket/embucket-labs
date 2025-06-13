WITH source AS (

    SELECT *
    FROM {{ref('xactly_period_source')}}

)

SELECT *
FROM source