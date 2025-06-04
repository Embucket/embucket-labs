WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_user_touchpoints_source', 'user_touchpoint_id') }}
    FROM {{ ref('bizible_user_touchpoints_source') }}

)

SELECT *
FROM source