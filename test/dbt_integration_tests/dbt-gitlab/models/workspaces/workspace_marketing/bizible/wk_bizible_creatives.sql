WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_creatives_source') }}
    FROM {{ ref('bizible_creatives_source') }}

)

SELECT *
FROM source