WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_facts_source') }}
    FROM {{ ref('bizible_facts_source') }}

)

SELECT *
FROM source