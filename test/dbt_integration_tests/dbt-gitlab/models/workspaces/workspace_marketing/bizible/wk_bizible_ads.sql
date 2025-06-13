WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_ads_source') }}
    FROM {{ ref('bizible_ads_source') }}

)

SELECT *
FROM source