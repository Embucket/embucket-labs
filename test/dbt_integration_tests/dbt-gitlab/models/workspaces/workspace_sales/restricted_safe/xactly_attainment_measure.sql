WITH source AS (

    SELECT *
    FROM {{ref('xactly_attainment_measure_source')}}

)

SELECT *
FROM source