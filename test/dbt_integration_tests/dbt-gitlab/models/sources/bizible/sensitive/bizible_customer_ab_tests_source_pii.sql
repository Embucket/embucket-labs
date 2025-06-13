WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_customer_ab_tests_source', 'visitor_id') }}
    FROM {{ ref('bizible_customer_ab_tests_source') }}

)

SELECT *
FROM source