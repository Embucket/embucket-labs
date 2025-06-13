WITH source AS (

    SELECT {{ hash_sensitive_columns('netsuite_entity_source') }}
    FROM {{ ref('netsuite_entity_source') }}

)

SELECT *
FROM source
WHERE is_fivetran_deleted = FALSE