WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_ad_campaigns_source') }}
    FROM {{ ref('bizible_ad_campaigns_source') }}

)

SELECT *
FROM source