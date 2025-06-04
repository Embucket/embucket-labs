WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_costs_source') }}
    FROM {{ ref('bizible_costs_source') }}

)

SELECT *
FROM source