{{ config(
    tags=["mnpi"]
) }}

WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'transactions') }}

), renamed AS (

    SELECT
      --Primary Key
      transaction_id::FLOAT                AS transaction_id,

      --Info
      entity_id::FLOAT                     AS entity_id,
      accounting_period_id::FLOAT          AS accounting_period_id,
      currency_id::FLOAT                   AS currency_id,
      transaction_type::VARCHAR            AS transaction_type,
      external_ref_number::VARCHAR         AS external_ref_number,
      transaction_extid::VARCHAR           AS transaction_ext_id,
      transaction_number::VARCHAR          AS transaction_number,
      memo::VARCHAR                        AS memo,
      tranid::VARCHAR                      AS document_id,
      opening_balance_transaction::VARCHAR AS balance,
      exchange_rate::FLOAT                 AS exchange_rate,
      weighted_total::FLOAT                AS total,
      status::VARCHAR                      AS status,
      due_date::TIMESTAMP_TZ               AS due_date,
      trandate::TIMESTAMP_TZ               AS transaction_date,
      sales_effective_date::TIMESTAMP_TZ   AS sales_effective_date

    FROM source

)

SELECT *
FROM renamed
