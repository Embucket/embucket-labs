WITH source AS (

    SELECT {{ hash_sensitive_columns('xactly_credit_source') }}
    FROM {{ ref('xactly_credit_source') }}

)

SELECT *
FROM source
