WITH source AS (

    SELECT *
    FROM {{ ref('bizible_attribution_touchpoints_source_pii') }}

)

SELECT *
FROM source