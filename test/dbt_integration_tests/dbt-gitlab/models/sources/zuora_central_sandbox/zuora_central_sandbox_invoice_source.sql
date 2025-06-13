WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'invoice') }}

), renamed AS(

    SELECT 
      id                                    AS invoice_id,
      -- keys
      account_id                            AS account_id,

      -- invoice metadata
      due_date                              AS due_date,
      invoice_number                        AS invoice_number,
      invoice_date                          AS invoice_date,
      status                                AS status,

      last_email_sent_date                  AS last_email_sent_date,
      posted_date                           AS posted_date,
      target_date                           AS target_date,


      includes_one_time                     AS includes_one_time,
      includes_recurring                    AS includes_recurring,
      includes_usage                        AS includes_usage,
      transferred_to_accounting             AS transferred_to_accounting,

      -- financial info
      adjustment_amount                     AS adjustment_amount,
      amount                                AS amount,
      amount_without_tax                    AS amount_without_tax,
      balance                               AS balance,
      credit_balance_adjustment_amount      AS credit_balance_adjustment_amount,
      payment_amount                        AS payment_amount,
      refund_amount                         AS refund_amount,
      tax_amount                            AS tax_amount,
      tax_exempt_amount                     AS tax_exempt_amount,
      comments                              AS comments,

      -- metadata
      created_by_id                         AS created_by_id,
      created_date                          AS created_date,
      posted_by                             AS posted_by,
      source                                AS source,
      source                                AS source_id,
      updated_by_id                         AS updated_by_id,
      updated_date                          AS updated_date,
      _FIVETRAN_DELETED                     AS is_deleted

    FROM source

)

SELECT *
FROM renamed
