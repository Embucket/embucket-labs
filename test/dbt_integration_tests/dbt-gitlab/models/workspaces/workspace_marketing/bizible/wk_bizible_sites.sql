WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_sites_source') }}
    FROM {{ ref('bizible_sites_source') }}

)

SELECT *
FROM source