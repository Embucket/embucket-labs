WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_urls_source') }}
    FROM {{ ref('bizible_urls_source') }}

)

SELECT *
FROM source