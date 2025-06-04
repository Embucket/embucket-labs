WITH source AS (

  SELECT *
  FROM {{ source('zuora', 'invoice_payment') }}

),

renamed AS (

  SELECT
    --keys
    id::VARCHAR                      AS invoice_payment_id,
    invoiceid::VARCHAR               AS invoice_id,
    accountid::VARCHAR               AS account_id,
    accountingperiodid::VARCHAR      AS accounting_period_id,

    --info
    billtocontactid::VARCHAR         AS bill_to_contact_id,
    cashaccountingcodeid::VARCHAR    AS cash_accounting_code_id,
    defaultpaymentmethodid::VARCHAR  AS default_payment_method_id,
    journalentryid::VARCHAR          AS journal_entry_id,
    journalrunid::VARCHAR            AS journal_run_id,
    parentaccountid::VARCHAR         AS parent_account_id,
    paymentid::VARCHAR               AS payment_id,
    paymentmethodid::VARCHAR         AS payment_method_id,
    paymentmethodsnapshotid::VARCHAR AS payment_method_snapshot_id,
    soldtocontactid::VARCHAR         AS sold_to_contact_id,
    createddate                      AS invoice_payment_date,

    --financial info
    amount::FLOAT                    AS invoice_payment_amount,
    refundamount::FLOAT              AS refund_amount,

    --metadata
    updatedbyid::VARCHAR             AS updated_by_id,
    updateddate::TIMESTAMP_TZ        AS updated_date,
    deleted::BOOLEAN                 AS is_deleted

  FROM source

)

SELECT *
FROM renamed
