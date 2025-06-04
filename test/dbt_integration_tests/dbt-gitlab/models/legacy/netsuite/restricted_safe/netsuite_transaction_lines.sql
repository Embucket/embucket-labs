WITH source AS (

    SELECT {{ hash_sensitive_columns('netsuite_transaction_lines_source') }}
    FROM {{ ref('netsuite_transaction_lines_source') }}

)

SELECT *
FROM source
WHERE non_posting_line != 'yes' --removes transactions not intended for posting
