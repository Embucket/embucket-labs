WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_facts_source', 'cost_key') }}
    FROM {{ ref('bizible_facts_source') }}

)

SELECT *
FROM source