WITH source AS (

    SELECT *
    FROM {{ref('xactly_plan_source')}}

)

SELECT *
FROM source