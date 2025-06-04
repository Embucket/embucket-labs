WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_advertisers_source') }}
    FROM {{ ref('bizible_advertisers_source') }}

)

SELECT *
FROM source