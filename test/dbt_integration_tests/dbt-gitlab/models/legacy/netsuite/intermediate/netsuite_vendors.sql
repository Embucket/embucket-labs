WITH source AS (

    SELECT {{ hash_sensitive_columns('netsuite_vendors_source') }}
    FROM {{ ref('netsuite_vendors_source') }}

)

SELECT *
FROM source