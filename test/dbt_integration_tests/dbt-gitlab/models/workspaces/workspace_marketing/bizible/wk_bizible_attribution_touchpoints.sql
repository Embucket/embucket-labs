WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_attribution_touchpoints_source') }}
    FROM {{ ref('bizible_attribution_touchpoints_source') }}

)

SELECT *
FROM source