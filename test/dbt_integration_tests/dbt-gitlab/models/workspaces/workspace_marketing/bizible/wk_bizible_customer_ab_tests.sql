WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_customer_ab_tests_source') }}
    FROM {{ ref('bizible_customer_ab_tests_source') }}

)

SELECT *
FROM source