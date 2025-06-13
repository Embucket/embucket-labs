WITH source AS (

    SELECT *
    FROM {{ ref('bizible_user_touchpoints_source_pii') }}

)

SELECT *
FROM source