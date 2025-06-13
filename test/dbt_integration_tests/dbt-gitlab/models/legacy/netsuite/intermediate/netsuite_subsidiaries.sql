WITH source AS (

    SELECT  {{ hash_sensitive_columns('netsuite_subsidiaries_source') }}
    FROM {{ ref('netsuite_subsidiaries_source') }}

)

SELECT *
FROM source