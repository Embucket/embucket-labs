WITH source AS (

    SELECT {{ hash_sensitive_columns('xactly_commission_source') }}
    FROM {{ ref('xactly_commission_source') }}

)

SELECT *
FROM source
