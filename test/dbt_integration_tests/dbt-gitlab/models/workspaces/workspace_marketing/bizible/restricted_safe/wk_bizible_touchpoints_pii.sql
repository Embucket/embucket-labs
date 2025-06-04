WITH source AS (

    SELECT *
    FROM {{ ref('bizible_touchpoints_source_pii') }}

)

SELECT *
FROM source