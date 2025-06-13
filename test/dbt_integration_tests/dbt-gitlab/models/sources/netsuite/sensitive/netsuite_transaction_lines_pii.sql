WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_transaction_lines_source') }}

), transaction_lines_pii AS (

    SELECT
      transaction_lines_unique_id,
      {{ nohash_sensitive_columns('netsuite_transaction_lines_source', 'memo') }}
    FROM source

)

SELECT *
FROM transaction_lines_pii
