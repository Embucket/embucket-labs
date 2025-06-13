WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_ad_groups_source') }}
    FROM {{ ref('bizible_ad_groups_source') }}

)

SELECT *
FROM source