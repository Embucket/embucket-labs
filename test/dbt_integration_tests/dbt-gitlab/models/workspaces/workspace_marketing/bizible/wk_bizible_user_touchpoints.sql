WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_user_touchpoints_source') }}
    FROM {{ ref('bizible_user_touchpoints_source') }}

)

SELECT *
FROM source