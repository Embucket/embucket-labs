WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_customers_source') }}

), customers_db_customers_pii AS (

    SELECT
      customer_id,
      {{ nohash_sensitive_columns('customers_db_customers_source', 'customer_email') }}
    FROM source

)

SELECT *
FROM customers_db_customers_pii
