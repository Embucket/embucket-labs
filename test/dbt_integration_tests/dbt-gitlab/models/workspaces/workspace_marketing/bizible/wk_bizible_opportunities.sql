WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_opportunities_source') }}
    FROM {{ ref('bizible_opportunities_source') }}

)

SELECT *
FROM source