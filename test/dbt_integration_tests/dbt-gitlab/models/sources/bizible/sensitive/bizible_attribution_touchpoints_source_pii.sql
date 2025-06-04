WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_attribution_touchpoints_source', 'attribution_touchpoint_id') }}
    FROM {{ ref('bizible_attribution_touchpoints_source') }}

)

SELECT *
FROM source