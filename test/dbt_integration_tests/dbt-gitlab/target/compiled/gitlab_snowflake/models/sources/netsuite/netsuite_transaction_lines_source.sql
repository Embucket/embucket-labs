

WITH source AS (

    SELECT *
    FROM EMBUCKET.netsuite_fivetran.transaction_lines

), renamed AS (

    SELECT
      md5(cast(coalesce(cast(transaction_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(transaction_line_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))
                                        AS transaction_lines_unique_id,
      --Primary Key
      transaction_id::FLOAT             AS transaction_id,
      transaction_line_id::FLOAT        AS transaction_line_id,

      --Foreign Keys
      account_id::FLOAT                 AS account_id,
      class_id::FLOAT                   AS class_id,
      department_id::FLOAT              AS department_id,
      subsidiary_id::FLOAT              AS subsidiary_id,
      company_id::FLOAT                 AS company_id,

      -- info
      memo::VARCHAR                     AS memo,
      receipt_url::VARCHAR              AS receipt_url,
      amount::FLOAT                     AS amount,
      gross_amount::FLOAT               AS gross_amount,

      LOWER(non_posting_line)::VARCHAR  AS non_posting_line

    FROM source

)

SELECT *
FROM renamed