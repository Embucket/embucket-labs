WITH source AS (

    SELECT *
    FROM {{ref('xactly_plan_assignment_source')}}

)

SELECT *
FROM source