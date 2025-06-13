WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'invoice_payment') }}

), renamed AS (

    SELECT
      --keys
      id::VARCHAR                              AS invoice_payment_id,
      invoice_id::VARCHAR                      AS invoice_id,
      account_id::VARCHAR                      AS account_id,
      accounting_period_id::VARCHAR            AS accounting_period_id,

      --info
      bill_to_contact_id::VARCHAR              AS bill_to_contact_id,
      cash_accounting_code_id::VARCHAR         AS cash_accounting_code_id,
      default_payment_method_id::VARCHAR       AS default_payment_method_id,
      journal_entry_id::VARCHAR                AS journal_entry_id,
      journal_run_id::VARCHAR                  AS journal_run_id,
      -- parent_account_id::VARCHAR               AS parent_account_id,
      payment_id::VARCHAR                      AS payment_id,
      payment_method_id::VARCHAR               AS payment_method_id,
      payment_method_snapshot_id::VARCHAR      AS payment_method_snapshot_id,
      sold_to_contact_id::VARCHAR              AS sold_to_contact_id,

      --financial info
      amount::FLOAT                            AS payment_amount,
      refund_amount::FLOAT                     AS refund_amount,

      --metadata
      updated_by_id::VARCHAR                   AS updated_by_id,
      updated_date::TIMESTAMP_TZ               AS updated_date,
      _FIVETRAN_DELETED::BOOLEAN               AS is_deleted

    FROM source

)

SELECT *
FROM renamed
