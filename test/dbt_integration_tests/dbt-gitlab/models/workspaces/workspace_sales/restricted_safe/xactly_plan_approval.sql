WITH source AS (

    SELECT *
    FROM {{ ref('xactly_plan_approval_source') }}

)

SELECT *
FROM source
