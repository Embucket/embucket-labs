WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_keywords_source') }}
    FROM {{ ref('bizible_keywords_source') }}

)

SELECT *
FROM source