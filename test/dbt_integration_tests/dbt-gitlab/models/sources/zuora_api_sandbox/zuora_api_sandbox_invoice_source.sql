WITH source AS (

    SELECT *
    FROM {{ source('zuora_api_sandbox', 'invoice') }}

), renamed AS(

    SELECT 
      id                                    AS invoice_id,
      -- keys
      accountid                             AS account_id,

      -- invoice metadata
      duedate                               AS due_date,
      invoicenumber                         AS invoice_number,
      invoicedate                           AS invoice_date,
      status,

      lastemailsentdate                     AS last_email_sent_date,
      posteddate                            AS posted_date,
      targetdate                            AS target_date,


      includesonetime                       AS includes_one_time,
      includesrecurring                     AS includesrecurring,
      includesusage                         AS includes_usage,
      transferredtoaccounting               AS transferred_to_accounting,

      -- financial info
      adjustmentamount                      AS adjustment_amount,
      amount,
      amountwithouttax                      AS amount_without_tax, 
      balance,
      creditbalanceadjustmentamount         AS credit_balance_adjustment_amount,
      paymentamount                         AS payment_amount,
      refundamount                          AS refund_amount,
      taxamount                             AS tax_amount,
      taxexemptamount                       AS tax_exempt_amount,
      comments,

      -- ext1, ext2, ext3, ... ext9

      -- metadata
      createdbyid                           AS created_by_id,
      createddate                           AS created_date,
      postedby                              AS posted_by,
      source                                AS source,
      source                                AS source_id,
      updatedbyid                           AS updated_by_id,
      updateddate                           AS updated_date,
      deleted                               AS is_deleted

    FROM source

)

SELECT *
FROM renamed
